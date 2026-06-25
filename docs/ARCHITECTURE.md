> 이 문서는 PR 마다 자동 생성/갱신됩니다.

# javi-forecast 아키텍처 가이드

이 문서는 `javi-forecast` 코드베이스에 처음 합류하는 사람 개발자를 위한 안내서입니다. `CLAUDE.md`가 AI 에이전트를 위한 압축된 레퍼런스라면, 이 문서는 같은 내용을 더 풀어서, 왜 그렇게 만들어졌는지에 대한 맥락과 함께 설명합니다.

## 1. 이 프로젝트는 무엇이고 왜 존재하는가

`javi-forecast`는 `javi` APM(애플리케이션 성능 모니터링) 플랫폼의 "AIOps 모델 서버"입니다. 한마디로, 들어오는 텔레메트리(스팬, 메트릭, 로그, 배포 이벤트)를 받아서 서비스별 RED 지표(Rate/Error/Duration — 요청률, 에러율, 응답시간 백분위수)를 시계열로 누적하고, 그 시계열을 바탕으로 가까운 미래를 예측하고, 예측이나 실측이 평소와 다르게 튀면 이상으로 감지해 알려주는 역할을 합니다.

왜 이런 서버가 따로 필요한가 하면, APM 데이터는 양이 많고 계속 흘러들어오는 스트림이라 "방금 무슨 일이 있었는지"를 보여주는 대시보드만으로는 부족합니다. `javi-forecast`는 그 시계열 위에 통계/머신러닝 모델(EWMA, ARIMA, Holt-Winters, Isolation Forest, STL, VAR, Granger 인과성 검정 등)을 얹어서 "30분 후 에러율이 어떻게 될 것 같다", "이 서비스의 이상이 다른 서비스에서 비롯된 것 같다", "이 JVM이 OOM까지 30분 남았다" 같은 한 단계 더 나간 판단을 내립니다. FastAPI 기반의 단일 비동기 프로세스이며, ClickHouse·Kafka·Redis 같은 무거운 인프라는 모두 선택적(optional)으로 동작하도록 설계되어 있어 로컬에서는 가볍게, 운영에서는 풀스택으로 띄울 수 있습니다.

## 2. 전체 아키텍처

핵심 그림은 "텔레메트리가 들어오는 입구(Kafka 또는 HTTP) → 메모리 안의 특징 저장소(FeatureStore류) → 그 위에서 계속 도는 백그라운드 엔진들 → 결과를 캐시/DB에 저장 → API로 노출" 입니다.

```
                         ┌────────────────────────────┐
                         │        javi-collector       │
                         │ (OTel 수집기, 별도 서비스)    │
                         └──────────────┬─────────────┘
                                         │ Kafka topics:
                                         │ spans.all / metrics / logs / deploys
                                         ▼
                         ┌────────────────────────────┐
   POST /v1/spans ─────► │   KafkaConsumerService      │ ◄───── POST /v1/metrics
   POST /v1/metrics ───► │  (app/consumer/*)            │ ◄───── POST /v1/metrics/jvm
                         └──────────────┬─────────────┘
                                        │ 토픽별 핸들러로 라우팅
                ┌───────────────────────┼───────────────────────┬─────────────┐
                ▼                       ▼                       ▼             ▼
        EventHandler           MetricEventHandler        LogEventHandler  DeployEventHandler
                │                       │                       │             │
                ▼                       ▼                       ▼             ▼
         FeatureStore           MetricFeatureStore         LogStore      DeploymentStore
       (RED 1분 버킷,           (커스텀 OTel 메트릭)        (ChromaDB,     (최근 배포 이력,
        72h 링버퍼,                                          선택적)        RCA에서 참조)
        Redis로 복제 가능)
                │
                │   ┌─────────────── 백그라운드 엔진들 (app/engine/*) ───────────────┐
                ├──►│ Forecaster        : RED 지표별 모델 선택+예측, 이상 예측 신호    │
                ├──►│ VarForecaster     : 서비스 간 VAR 다변량 예측                   │
                ├──►│ GrangerAnalyzer   : 서비스 간 인과관계(Granger) 추론             │
                ├──►│ BaselineComputer  : 요일×시간대 베이스라인 계산 (ClickHouse)     │
                ├──►│ AnomalyDetector   : ClickHouse 기반 z-score/IsolationForest 이상감지│
                ├──►│ RCAEngine         : 이상 + 스팬/토폴로지/배포로 원인 가설 생성     │
                ├──►│ JvmAnalyzer       : OOM/GC/스레드 누수 예측                      │
                └──►│ BurnRateAnalyzer  : SLO 소진율 알림                              │
                    └─────────────────────┬───────────────────────────────────────────┘
                                          │ 결과 저장
                  ┌───────────────────────┼─────────────────────────┐
                  ▼                       ▼                         ▼
            ForecastStore            AlertStore              DependencyMap /
          (TTL 메모리 캐시)      (in-memory + ClickHouse)    IncidentStore / LogStore
                  │                       │                         │
                  └───────────────────────┴─────────────────────────┘
                                          │
                                          ▼
                          FastAPI 라우터 (app/api/*)
                 /api/forecast, /api/alerts, /api/dependencies,
                 /api/jvm, /api/topology, /api/rag, /metrics ...
                                          │
                                          ▼
                                  사용자/대시보드/다른 서비스
```

ClickHouse, Kafka, Redis, RAG(LLM) 기능은 모두 환경변수 플래그로 켜고 끌 수 있고, 꺼져 있으면 해당 기능만 조용히 비활성화된 채로 나머지는 정상 동작합니다 (자세한 내용은 6절 참고).

## 3. 데이터/요청 흐름

크게 두 가지 흐름이 있습니다: **(A) 텔레메트리가 들어와서 메트릭이 쌓이는 흐름**과 **(B) 그 메트릭을 바탕으로 예측/이상감지가 돌고 API로 나가는 흐름**입니다.

### (A) 입력 → 저장

1. `javi-collector`가 OTel 데이터를 Kafka 토픽(`spans.all`, `metrics`, `logs`, `deploys`)으로 발행합니다. 로컬 개발이나 단순 연동 시에는 Kafka 없이 `POST /v1/spans`, `POST /v1/metrics`, `POST /v1/metrics/jvm` 같은 HTTP 엔드포인트로 직접 넣을 수도 있습니다.
2. `KafkaConsumerService`(`app/consumer/kafka_consumer.py`)가 토픽을 구독하고, 메시지를 종류별로 해당 핸들러(`EventHandler`, `MetricEventHandler`, `LogEventHandler`, `DeployEventHandler`)에 위임합니다. 메시지는 JSON이고 `schema_version` 필드가 있으며, 필드명은 항상 snake_case입니다(collector와의 계약).
3. 각 핸들러는 받은 이벤트를 알맞은 저장소에 반영합니다: 스팬 → `FeatureStore`(1분 단위 RED 버킷으로 집계) + `SpanTopologyTracker`(호출 그래프) + `ServiceRegistry`, 메트릭 → `MetricFeatureStore`, 로그 → `LogStore`, 배포 → `DeploymentStore`.
4. 컨슈머는 디스패치가 끝난 뒤에 수동으로 오프셋을 커밋합니다(at-least-once). 즉 처리 중 크래시가 나면 같은 메시지가 중복 처리될 수 있습니다 — 자세한 트레이드오프는 7절을 참고하세요.

### (B) 저장 → 예측/이상감지 → API

5. `Forecaster`(`app/engine/forecaster.py`)가 설정된 주기(`FORECAST_INTERVAL_SECONDS`, 기본 60초)마다 서비스×RED 지표 조합마다 `FeatureStore`에서 과거 구간을 읽고, `selector.select_model()`로 EWMA/ARIMA/Holt-Winters 중 하나(또는 auto 모드면 교차검증으로 가장 좋은 것)를 골라 `FORECAST_HORIZON_MINUTES`(기본 30분) 만큼 미래를 예측합니다.
6. 예측 결과는 과거 베이스라인과 비교되어 z-score 기반으로 "곧 이상이 생길 것 같다"는 신호(`AnomalyPredictor`)를 만들 수 있고, 그 결과는 `ForecastStore`(TTL 캐시)에 저장되고 필요하면 `WebhookAlerter`를 통해 Slack/웹훅으로 알립니다.
7. 같은 루프 안에서 `IsolationForestDetector`(다변량 이상감지)와 `STLAnomalyDetector`(계절성 분해 기반 이상감지)도 함께 돕니다.
8. ClickHouse가 켜져 있다면 별도의 `AnomalyDetector`(ClickHouse에 쌓인 1분 집계와 베이스라인 테이블을 비교해 이상을 잡아 `apm.anomalies`에 기록)와 `RCAEngine`(이상 건마다 관련 스팬·토폴로지 이웃·근처 배포를 찾아 원인 가설을 만들어 `apm.rca_reports`에 기록), `BaselineComputer`(요일×시간대 베이스라인을 주기적으로 재계산)가 함께 동작합니다.
9. `VarForecaster`와 `GrangerAnalyzer`는 서비스 하나가 아니라 서비스들 사이의 관계를 봅니다 — VAR은 여러 서비스의 에러율을 함께 모델링해 예측하고, Granger는 "A 서비스의 에러율 변화가 B 서비스의 변화를 통계적으로 선행하는가"를 검정해 `DependencyMap`(인과 그래프)을 갱신합니다. 이 그래프는 RCA와 `/api/dependencies`에서 쓰입니다.
10. `JvmAnalyzer`와 `BurnRateAnalyzer`는 각각 JVM 힙/GC/스레드 추세로 OOM 등을 예측하고, SLO 에러 버짓 소진 속도를 멀티 윈도우(5분/60분)로 계산해 알립니다.
11. 마지막으로 모든 결과는 FastAPI 라우터(`app/api/*`)를 통해 외부에 노출됩니다 — 예측치, 알림, 의존성 그래프, 토폴로지, JVM 상태, 그리고 (RAG가 켜져 있다면) 자연어 질의를 ClickHouse SQL로 바꿔주는 `/api/rag/query` 등.

## 4. 핵심 디렉터리·모듈

코드는 책임에 따라 명확히 나뉘어 있습니다. 같은 디렉터리에 있는 모듈들은 대체로 "같은 종류의 일을 하는 백그라운드 엔진" 또는 "같은 계층의 저장소"입니다.

```
app/
├── main.py        # FastAPI lifespan: 모든 컴포넌트의 생성/시작/종료 순서를 결정하는 곳
├── config.py      # pydantic-settings 기반 환경변수 설정 (모든 토글의 단일 출처)
├── alerter/       # WebhookAlerter — 일반 웹훅 + Slack 웹훅으로 알림 발송, 쿨다운 포함
├── consumer/      # Kafka 컨슈머와 토픽별 이벤트 핸들러 (입력 게이트)
├── engine/        # 모든 백그라운드 엔진과 인메모리 저장소 — 이 프로젝트의 핵심부
│   └── models/    # 예측 알고리즘 자체 (EWMA/ARIMA/Holt-Winters) + 모델 선택기
├── anomaly/       # 순수 통계/ML 이상감지 알고리즘 (z-score, IsolationForest, STL)
├── store/         # 외부 저장소 클라이언트 (ClickHouse) + 인메모리 예측 캐시
├── rag/           # 선택적 LLM 기능: 자연어→SQL, 인시던트/로그 시맨틱 검색
├── models/        # Pydantic 데이터 모델 (Kafka/HTTP 페이로드 + 내부 결과 타입)
└── api/           # FastAPI 라우터 — 위 모든 엔진/저장소를 HTTP로 노출
```

이렇게 나눈 이유는 관심사 분리가 명확합니다: `consumer/`는 "데이터가 어떻게 들어오는가"만 알고, `engine/`은 "그 데이터로 무엇을 계산하는가"만 알고, `api/`는 "계산된 결과를 어떻게 보여주는가"만 압니다. `anomaly/`가 `engine/`과 분리된 이유는, 순수 알고리즘(입력→출력만 있는 통계 함수)과 그 알고리즘을 주기적으로 돌리며 상태를 관리하는 엔진을 구분하기 위함으로 보입니다.

각 모듈의 역할을 조금 더 풀면:

- **`app/engine/feature_store.py` (`FeatureStore`)** — 서비스별로 1분 단위 RED 메트릭을 쌓는 인메모리 링버퍼(72시간치, `deque`)입니다. 들어오는 스팬을 그 순간의 "열린 버킷"에 누적하다가 분(minute) 경계가 지나면 버킷을 확정해 시리즈에 추가합니다. 서비스 수가 너무 많아지면(`MAX_FEATURE_STORE_SERVICES`) LRU로 오래된 서비스를 내립니다. `REDIS_URL`이 설정되어 있으면 같은 데이터를 Redis에도 써서, 여러 레플리카가 떠 있을 때 재시작 시 복구하거나 다른 레플리카와 데이터를 공유할 수 있게 합니다.
- **`app/engine/forecaster.py` (`Forecaster`)** — 가장 중심이 되는 백그라운드 루프입니다. 매 사이클마다 서비스×RED 지표 조합을 동시성 제한(세마포어)을 걸고 순회하며, 모델을 고르고, 예측하고, 이상 신호가 있으면 알리고, 결과를 캐시에 넣습니다. 같은 루프 안에서 커스텀 메트릭 예측, Isolation Forest, STL 사이클도 함께 실행합니다.
- **`app/engine/selector.py`** — "auto" 모드일 때 EWMA/ARIMA/Holt-Winters 세 모델을 모두 학습시켜 검증셋 MSE가 가장 낮은 것을 고릅니다. 명시적으로 모델을 지정하면(`DEFAULT_MODEL=ewma` 등) 이 평가 과정을 건너뛰어 비용을 줄입니다.
- **`app/engine/models/`** — 실제 예측 알고리즘 3종(EWMA, ARIMA, Holt-Winters)과 공통 인터페이스(`BaseForecaster`)가 있습니다. EWMA는 순수 numpy로 구현된 더블 지수평활(추세 포함)이라 실시간·저지연에 적합하고, ARIMA/Holt-Winters는 statsmodels를 써서 추세·계절성이 더 뚜렷한 시리즈에 적합합니다.
- **`app/engine/rca_engine.py` (`RCAEngine`)** — ClickHouse에 쌓인 최근 이상 건들을 주기적으로 가져와서, 관련된 에러 스팬, 토폴로지상 이웃 서비스, 근처 시점의 배포 이벤트를 모아 규칙 기반으로 원인 가설 문장을 만듭니다. RAG가 켜져 있으면 이를 더 자연어스러운 인시던트 요약으로 감쌀 수 있습니다.
- **`app/engine/var_forecaster.py` / `granger_analyzer.py` / `dependency_map.py`** — 서비스를 "하나씩" 보지 않고 "서로 어떻게 영향을 주는지"를 보는 계층입니다. VAR은 여러 서비스의 에러율 시계열을 한 모델로 묶어 예측하고, Granger 분석은 통계적 인과성 검정을 통해 방향성 있는 그래프(`DependencyMap`)를 만듭니다. 이 그래프는 RCA에서 "이 이상이 어디서 흘러왔을 가능성이 있는가"를 답하는 데 쓰입니다.
- **`app/engine/jvm_analyzer.py` / `jvm_feature_store.py`** — JVM 힙 사용률을 선형 회귀로 추세 외삽해 OOM까지 남은 시간을 추정하고, GC 정지시간 급증이나 스레드 수 단조 증가(누수 의심)도 감지합니다.
- **`app/engine/burn_rate_analyzer.py`** — SLO(가용성 목표) 대비 에러 버짓이 얼마나 빠르게 소진되는지를 멀티 윈도우(빠른 창/느린 창)로 계산하는, Google SRE의 멀티윈도우 burn-rate 알림 패턴을 따릅니다.
- **`app/engine/baseline_computer.py` / `baseline_store.py`** — "이 서비스는 평소 화요일 오후 3시에 어느 정도 트래픽/에러율을 보이는가"를 ClickHouse 집계(28일치)로 계산해두고(`baseline_computer.py`), 그 결과를 메모리에 캐싱해(`baseline_store.py`) z-score 계산 시 "그냥 평균"이 아니라 "그 요일·시간대의 평균"과 비교할 수 있게 합니다.
- **`app/anomaly/`** — 순수 알고리즘 모음입니다. `predictor.py`는 예측값을 베이스라인과 비교하는 z-score 로직, `isolation_forest.py`는 RED 5차원 벡터에 대한 sklearn IsolationForest 래퍼, `stl_detector.py`는 계절성 분해(LOESS) 후 잔차의 z-score로 이상을 잡는 로직입니다.
- **`app/store/clickhouse.py`** — `clickhouse-connect`(HTTP, 9000번 네이티브 포트가 아니라 8123번 HTTP 포트를 씀)를 감싼 비동기 래퍼입니다. 블로킹 호출은 executor로 돌립니다. `apm.spans`, `apm.red_baseline`, `apm.anomalies`, `apm.alerts`, `apm.rca_reports` 같은 테이블에 의존합니다.
- **`app/store/forecast_store.py`** — 예측 결과를 1시간 TTL로 메모리에만 캐싱합니다(영속화하지 않음 — 재시작하면 사라지고 다음 사이클에 다시 채워짐).
- **`app/rag/`** — `RAG_ENABLED`가 꺼져 있으면 이 디렉터리의 기능은 전혀 동작하지 않는 선택적 계층입니다. `text_to_sql.py`는 Anthropic Claude에게 ClickHouse 스키마 컨텍스트를 주고 자연어 질문을 SELECT 전용 SQL로 변환시킨 뒤(DDL/DML은 정규식으로 차단) 실행합니다. `incident_store.py`/`log_store.py`는 ChromaDB(없으면 인메모리 폴백)에 인시던트/로그를 저장해 의미 기반 검색을 지원합니다.
- **`app/api/`** — 각 파일이 한 가지 주제의 라우터입니다(`forecast.py`, `alerts.py`, `dependency.py`, `jvm.py`, `topology.py`, `rag.py` 등). 새 기능을 추가할 때 "엔진/저장소를 만들고 → 그걸 노출하는 라우터를 하나 추가" 하는 패턴이 일관되게 반복됩니다.

## 5. 로컬 개발 시작하기

가장 빠른 길은 ClickHouse나 Kafka 없이 띄우는 것입니다:

```bash
pip install -e ".[dev]"
make dev
# 내부적으로: DISABLE_CLICKHOUSE=true KAFKA_ENABLED=false LOG_LEVEL=debug \
#   uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload
```

이 모드에서는 ClickHouse 의존 엔진들(`AnomalyDetector`, `RCAEngine`, `BaselineComputer`)이 조용히 비활성화되고, Kafka 컨슈머도 뜨지 않습니다. 데이터를 넣고 싶으면 `POST /v1/spans`, `POST /v1/metrics` 같은 HTTP 엔드포인트로 직접 보내면 됩니다. API 문서는 `http://localhost:8080/docs`에서 바로 확인할 수 있습니다.

풀스택으로 보고 싶다면:

```bash
make docker-up    # ClickHouse + Kafka(+Zookeeper) + javi-forecast 컨테이너를 빌드/기동
make run          # (docker compose가 떠 있는 상태에서) 풀스택 설정으로 로컬 uvicorn 실행
make docker-down
```

자주 쓰는 그 외 명령:

```bash
make lint      # ruff check app/ && mypy app/ --ignore-missing-imports
make fmt       # ruff format app/
make test                        # 전체 테스트
pytest tests/test_rca_engine.py  # 파일 단위
pytest tests/ -v -k "test_name"  # 이름으로 단일 테스트
make test-cov  # 커버리지 포함
make health    # 떠 있는 서버에 헬스체크
```

ClickHouse·Redis·Anthropic API 키 같은 자격증명은 `.env`로 관리하며(`.env.example` 참고), 커밋하지 않습니다. RAG/LLM 기능을 쓰려면 `RAG_ENABLED=true`와 `ANTHROPIC_API_KEY`가 둘 다 필요합니다.

## 6. 알아두면 좋은 함정과 트레이드오프

- **Kafka 오프셋은 at-least-once로 커밋됩니다.** `KafkaConsumerService`는 메시지를 핸들러에 디스패치한 *뒤에* 오프셋을 커밋합니다(자동 커밋 비활성화). 즉 처리 도중 프로세스가 죽으면 같은 메시지가 재처리될 수 있습니다. 손상된 메시지도 일단 커밋해버리는데, 이는 "poison pill"(영원히 실패하는 메시지)이 컨슈머 전체를 멈추는 것을 막기 위한 설계로 보입니다. `FeatureStore`는 같은 분(minute) 타임스탬프 중복은 걸러내지만, 그 외의 중복 처리까지 막아주지는 않습니다.
- **FeatureStore의 메모리-우선 + Redis 동기화는 엄격한 일관성을 보장하지 않습니다.** 로컬에 먼저 쓰고 Redis 반영은 별도 비동기 작업으로 흘려보내는 방식이라, 그 사이에 크래시가 나면 레플리카 간 데이터가 잠시 어긋날 수 있습니다. 처리량을 우선시하고 최종 일관성(eventual consistency)을 받아들이는 설계입니다. 멀티 레플리카로 운영한다면 `REDIS_URL`과 레플리카별 `INSTANCE_ID`를 꼭 설정하세요.
- **`auto` 모델 선택은 비용이 있습니다.** 모델을 명시하지 않고 `DEFAULT_MODEL=auto`로 두면, 매 예측 사이클마다 서비스×지표 조합 전체에 대해 EWMA/ARIMA/Holt-Winters 세 모델을 모두 학습시켜 비교합니다. 서비스가 많아지면 사이클 시간이 늘어날 수 있으니, 운영 환경에서 지연이 문제라면 명시적인 모델(예: `ewma`)로 고정하는 것도 고려할 수 있습니다.
- **ChromaDB 영속 경로는 컨테이너 재시작에 취약합니다.** `IncidentStore`/`LogStore`는 기본적으로 `/data/javi_incidents`, `/data/javi_logs`에 데이터를 둡니다. 쿠버네티스 등에서 이 경로에 PVC를 마운트하지 않으면 재시작 시 인시던트/로그 검색 이력이 사라지고, ChromaDB 자체가 없으면 인메모리 폴백(유사도 검색 없이 단순 문자열 매칭)으로 동작합니다.
- **여러 엔진이 N² 비용 패턴을 가집니다.** `GrangerAnalyzer`는 서비스 쌍마다 인과성 검정을 돌리므로 서비스 수가 늘면 비용이 제곱으로 늘어납니다. 서비스가 매우 많은 환경에서는 `GRANGER_ENABLED=false`로 끄는 것도 선택지입니다.
- **ClickHouse 의존 엔진들은 ClickHouse가 없으면 조용히 꺼집니다.** `AnomalyDetector`, `RCAEngine`, `BaselineComputer`는 ClickHouse 연결을 전제로 하며, `DISABLE_CLICKHOUSE=true`(로컬 개발 기본값)에서는 비활성화됩니다. 운영에서 이 기능들이 필요한데 ClickHouse 연결이 실패했다면, 에러가 시끄럽게 나지 않고 그냥 기능이 빠진 채로 서버가 떠 있을 수 있다는 점을 기억해두세요.
- **이 서버는 단일 비동기 프로세스입니다.** uvicorn은 기본적으로 한 프로세스로 떠 있고, CPU 위주 작업(모델 학습, Granger 검정, IsolationForest 재학습)은 executor 스레드로 돌려 이벤트 루프를 막지 않으려 하지만, 동시 부하가 크면 그 executor 자체가 병목이 될 수 있습니다. 스케일이 필요하면 레플리카를 늘리는 방향(이 경우 Redis 공유 상태가 필요)을 고려해야 합니다.

---

*이 문서는 코드를 읽고 작성된 것으로, 추측이 필요했던 부분은 본문에서 "~로 보입니다" 등으로 표시했습니다. 실제 동작과 다른 부분을 발견하면 언제든 이 문서를 갱신해주세요.*
