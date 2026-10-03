## [0.2.1](https://github.com/prostojs/redisjm/compare/v0.2.0...v0.2.1) (2026-10-03)


### Features

* attempt number on job event payloads ([8c32c5d](https://github.com/prostojs/redisjm/commit/8c32c5dfe45ec50d4b4d0a9313905348c372349d))



# [0.2.0](https://github.com/prostojs/redisjm/compare/v0.1.1...v0.2.0) (2026-10-03)



## [0.1.1](https://github.com/prostojs/redisjm/compare/v0.1.0...v0.1.1) (2026-07-10)


### Bug Fixes

* self-heal staled-but-alive runs instead of freezing progress and losing attrs ([f4ea339](https://github.com/prostojs/redisjm/commit/f4ea3394cc3f378e32c1891f34563cb4d2cf83d2))



# [0.1.0](https://github.com/prostojs/redisjm/compare/v0.0.4...v0.1.0) (2026-07-09)


### Features

* add delayed runs and retries with backoff via a per-group delayed zset ([21663c7](https://github.com/prostojs/redisjm/commit/21663c73936a500d81148465c259605922716c51))
* add per-instance concurrency and cooperative cancellation via AbortSignal ([e70c9cc](https://github.com/prostojs/redisjm/commit/e70c9cca79020bbf729c0dce10f9f98d088b0da4))
* add stats/queueSize introspection, Job.queueFirst, isLocked, and 60s retention default ([0f61a1d](https://github.com/prostojs/redisjm/commit/0f61a1d3e53facbc0792ad51104b5090cc22678d))
* fence executions with per-run tokens and isolate hook failures from job outcomes ([3ebf728](https://github.com/prostojs/redisjm/commit/3ebf7289eac81e7a38b646de89b647a7dadc8b4f))
* reclaim orphaned locks, purge corrupt records, and scan incrementally during maintenance ([141ca6f](https://github.com/prostojs/redisjm/commit/141ca6f66ab6a7a6eaa279f9bf8b9c7e4ac1a8c6))



## [0.0.4](https://github.com/prostojs/redisjm/compare/v0.0.3...v0.0.4) (2026-07-09)


### Features

* add lane type surface and lane-strategy options ([8244bd0](https://github.com/prostojs/redisjm/commit/8244bd03eac7bc2d7e6a7d37fbc0f083befdbb8a))
* poll subscribed lanes with LMPOP and reserved maintenance lane ([7b88d2b](https://github.com/prostojs/redisjm/commit/7b88d2bbff839af9551ea33ce8bd01b0b45bf187))
* relocate a maintenance job stranded on the legacy queue at cutover ([42eb9f4](https://github.com/prostojs/redisjm/commit/42eb9f4c8e6e532109f72cddecca456765cfef34))
* route enqueue and write-side ops per lane ([4a6437c](https://github.com/prostojs/redisjm/commit/4a6437c679f823d5c4d2e658f056680561165a99))



## [0.0.3](https://github.com/prostojs/redisjm/compare/v0.0.2...v0.0.3) (2026-06-26)


### Features

* default error logging, bounded unknown-job requeue, awaitable stop, resilience hardening ([42a39a9](https://github.com/prostojs/redisjm/commit/42a39a904d85dd21740a1edf89bcf763dac2d1f9))



## [0.0.2](https://github.com/prostojs/redisjm/compare/v0.0.1...v0.0.2) (2026-06-10)


### Features

* auto-maintenance scheduling and orphaned queued job detection ([155cb2b](https://github.com/prostojs/redisjm/commit/155cb2bdbc60989fa19051d4c5cb4b5c556641e1))



## 0.0.1 (2026-02-10)


### Features

* redisjm implemented ([178a97f](https://github.com/prostojs/redisjm/commit/178a97f8989f01ad05c47b61ad0d00fadd2989cd))



