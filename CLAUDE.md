# CLAUDE.md — SnapShotServer

ARSSnapShotServer: ARSAgent 클릭 스냅샷(base64 PNG)을 HTTP 로 받아 Cassandra `snapshot`·`snapshotlist` 에 쓰고, ScenarioEditor 의 월 목록·이미지 조회(`DIR`·`GETFILE`)에 답하는 Scala/Akka-HTTP 서버. 역할·연동은 루트 `docs/system-landscape.md` "ARSSnapShotServer" 항목이 정본.

## 저장소와 사내 동기화

- **git 저장소다** (2026-10-26 생성, 브랜치 `main`). **원격은 GitHub 비공개 저장소 `origin` = `WorkforRPASolution/SnapShotServer`** (2026-10-26 첫 푸시). 사내에서 clone 해 사내 소스와 diff 하는 데 쓴다. **푸시는 사용자가 요청할 때만 한다.**
- **기준점 `8e9ff50`** = 2026-10-26 로컬 사본 그대로(src + 옮겨 적은 원본 pom). 사내 반영용 패치는 이 커밋과의 diff 로 뽑는다. 바로 다음 `e953602` 는 Mac 빌드용 pom 수정이다 — 좌표 오타 수정과, 사내와 다를 수 있는 변경(Akka 2.8.6→2.5.31 등)이 섞여 있으니 사내 pom 과 대조해 가른다.
- **사내가 정본이다.** 소스는 사내 코드를 옮겨 적은 것이다. 옮겨 적기 오타(import 누락, `JavaConversions`→`JavaConverters` 치환 등)는 사내 원본과 대조한 뒤 고친다. **사내 코드 자체의 결함은 Mac 도 사내와 같게 두고 동기화를 끝낸 뒤 따로 검토한다** (사용자 결정 2026-10-11). 그 목록은 백로그 에픽 SS-001 아래 task 들이다.
- 고친 뒤에는 작업 단위로 바로 커밋한다. 커밋 메시지는 한국어, 옮겨 적기 오타 수정은 `전사 교정:` 으로 시작한다(InterfaceServer 와 같은 규칙 — 사내 패치에서 뺄 커밋을 가리기 위해).
- 사내 코드에도 생성 코드가 없는 액터: `BatchWriter`·`TransformerMsg`·`Logger` (`Logger` 사용자 확인 2026-10-26). SS-007·SS-011.

## 빌드

- JDK 8 (Zulu 8) + Maven. Scala 2.12.8, Akka 2.5.31, akka-http 10.1.12 — 형제 프로젝트(InterfaceServer·CommandServer)와 같은 세대.
- `mvn clean package` → `target/ARSSnapShotServer.jar` + `target/lib/` (의존 jar). 테스트는 scalatest-maven-plugin 이 실행하고 surefire 는 꺼져 있다.
- IntelliJ 는 `pom.xml` 을 **Add as Maven Project** 로 연결해야 라이브러리·Scala SDK 가 붙는다. `.idea/` 는 일반 Java 모듈로 만들어진 상태라 pom 을 자동으로 읽지 않는다. `.idea/` 는 git 에서 제외한다.
- 실행에 필요한 것: `-DEEG_BASE=<root>` 와 `<root>/conf/ARSSnapShotServer/{ARSSnapShotServer.json, application.conf}`, env `HOST_IP`·`HTTP_PORT`·`Username`·`Password`. 진입점은 `Master.main start|stop` 또는 commons-daemon(prunsrv) 용 `com.sec.eeg.daemon.Application`.

## 백로그

**접두사 `SS-`** (2026-10-11 신설). 지침은 전역 `~/.claude/CLAUDE.md`. 이 세션이 `SS-` 항목을 `add`/`update` 한다. 에픽 SS-001 = 사내 코드 동기화 후 검토.

## 테스트

`src/test/scala` 는 아직 없다. 새 코드는 루트 CLAUDE.md 의 TDD 규칙을 따른다(scalatest).
