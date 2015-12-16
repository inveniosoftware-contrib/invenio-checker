Installation
============
Invenio-Checker can be installed with the following steps:

1. Install this package: ``pip install invenio-checker``
2. Append ``"invenio_checker"`` to ``base.config.PACKAGES`` of your overlay.
3. Because there is currently no per-module way of declaring javascript requirements and shims, you will also have to append the ones required by ``invenio-checker`` to your overlay.
    - Make the following modifications in your overlay's ``base/static/js/settings.js``:
      - To ``require.config:paths``, append::

            "watable": "vendors/watable/jquery.watable",
            "bootstrap-datepicker-eyecon": "vendors/bootstrap-datepicker-eyecon/js/bootstrap-datepicker",
            "jqcron": "vendors/jqcron/src/jqCron",
            "jqcron.en": "vendors/jqcron/src/jqCron.en",

      - To ``require.config:shim``, append::

            "watable": {
                deps: ["jquery", "bootstrap"],
                exports: "$.fn.WATable"
            },
            "bootstrap-datepicker-eyecon": {
                deps: ["jquery", "bootstrap"],
                exports: "$.fn.datepicker"
            },
            "jqcron.en": {
                deps: ["jquery", "jqcron"],
            },
            "jqcron": {
                deps: ["jquery"],
                exports: "$.fn.jqCron"
            },
4. To enable the scheduler, add the following to your overlay's ``config.py``:

.. code-block:: python

    CELERYBEAT_SCHEDULE = {}  # If name not already defined in scope

    from invenio_checker.config import CHECKER_CELERYBEAT_SCHEDULE
    CELERYBEAT_SCHEDULE.update(CHECKER_CELERYBEAT_SCHEDULE)
