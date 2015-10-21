Installation
============
Invenio-Checker can be installed with the following steps:

- `pip install invenio-checker`
- Append `invenio_checker` to `base.config.PACKAGES` of your overlay.
- TODO: jqcron, watable, bootstrap-datepicker-eyecon extension
- To enable the scheduler, import and add
  `invenio_checker.config:CHECKER_CELERYBEAT_SCHEDULE` to your
  `CELERYBEAT_SCHEDULE`.
