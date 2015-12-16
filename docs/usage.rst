..
    This file is part of Invenio.  Copyright (C) 2015 CERN.

    Invenio is free software; you can redistribute it and/or modify it under
    the terms of the GNU General Public License as published by the Free
    Software Foundation; either version 2 of the License, or (at your option)
    any later version.

    Invenio is distributed in the hope that it will be useful, but WITHOUT ANY
    WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
    FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
    details.

    You should have received a copy of the GNU General Public License along
    with Invenio; if not, write to the Free Software Foundation, Inc., 59
    Temple Place, Suite 330, Boston, MA 02111-1307, USA.

    In applying this license, CERN does not waive the privileges and immunities
    granted to it by virtue of its status as an Intergovernmental Organization
    or submit itself to any jurisdiction.


Usage
=====

Task Configuration
------------------

Configuration variables of tasks are documented in the ``CheckerRule`` class in
``models.py``:

.. literalinclude:: ../invenio_checker/models.py
    :lines: 101-
    :end-before: @db.hybrid_property
    :language: python
    :linenos:
    :lineno-match:

Reporter Configuration
----------------------

Configuration variables of tasks are documented in the ``CheckerRule`` class in
``models.py``:

.. literalinclude:: ../invenio_checker/models.py
    :lines: 536-
    :end-before: @db.hybrid_property
    :language: python
    :linenos:
    :lineno-match:

Writing Check Files
-------------------

The check files (and specifically the function that lives inside them) is all
the business logic that you will get the checker to execute.

Defining the function that will be ran
``````````````````````````````````````

- Here is a simple check file that writes to writes to ``stdout``:

    .. code-block:: python

        def check_write_to_stdout():
            print 'Hello, I executed.'

    Note that the check's function name must begin with ``check``.

- We can also define a class around this function, even though we'll get to why
  this is useful later. This example acts the same as the last one:

    .. code-block:: python

        class CheckWritingToStdout(object):
            def check_write_to_stdout(self):
                print 'Hello, I executed.'

    Don't forget the ``self`` argument!

Using fixtures
``````````````

- The last examples were not very useful because ``stdout`` is ignored by the
  checker.  We should use the checker's logging capabilities instead. This way
  not only will we find the message in the checker's native logs, but the
  ``reporters`` that this ``check``'s ``tasks`` has been associated with will
  be notified about this log:

    .. code-block:: python

        def check_write_to_stdout(log):
            log('Hello, I executed.')

    Note that we didn't have to import ``log`` or do anything special to use
    it. This is because ``fixtures`` (arguments in your function definition)
    are automatically detected for you.

    Still not the most useful example so let's move on.

- This time we'd like to know which records this ``chunk`` has been called
  upon. Let's use the relevant fixture for that.

    .. code-block:: python

        def check_log_recids_we_were_called_with(log, batch_recids):
            assert isinstance(batch_recids[0], int)
            log('Executed for the following record IDs: {}'.format(batch_recids))

    Great, we didn't have to worry about fetching these record IDs from
    somewhere. The ``checker`` put them in ``batch_recids`` for us.

    Note that these two function signatures result in exactly the same result
    and work equally well, as argument order and count does not matter:

    .. code-block:: python

        def check_log_recids_we_were_called_with(log, batch_recids)
        def check_log_recids_we_were_called_with(batch_recids, log)

Modifying records
`````````````````

- Let's naively modify some records. (Please don't use this example in your
  code):

    .. code-block:: python

        def check_modify_all_records(batch_recids):
            from invenio_records.api import get_record
            from invenio_ext.sqlalchemy import db

            for rec_id in batch_recids:
                rec = get_record(rec_id)
                rec['key'] = 'value'
                rec.commit()
            db.session.commit()

    Let's see why this is not the recommended way of working:
        - We had to import from modules we shouldn't have to know about.
        - We had to do a record commit ourselves. Mind you, this process runs
          in ``celery``, so this is dangerous on its own.
        - We wrote our own loop for going over ``batch_recids``, but this
          something we expect to do frequently in our ``checks``.

- For these reasons the ``checker`` provides a number of helper fixtures. These
  are ``record``, ``get_record`` and ``search``. All of them rely on the same
  mechanism for abstracting your code away from the problems listed above.
  Let's see an example that uses all of them.

    .. code-block:: python

        def check_records_and_do_funny_things(record, get_record, search):
            # Entire previous function
            record['key'] = 'value'

            # Demonstrate dynamic `get_record`
            for support_record_id in record['referenced_records']:
                get_record(support_record_id)['is_referenced'] = True

            # Demonstrate search via fixture
            value_to_search_for = record['search_for_this']
            for record_from_search in search(value_to_search_for):
                record_from_search['found_via_search'] = True

    Note how the use of the ``record`` fixture automatically means that our
    function will be called ``batch_recids`` times, with a different record
    each time.

    Also note that even if one of these fixtures returned the same record
    twice, you will get the same object back, as you modified it earlier.

    Every single modification that this function made will be committed for us
    at the end of the current ``batch`` of the running ``task_execution``,
    unless one of the following is true:

        - This is a ``dry_run``,
        - We have enabled ``confirm_hash_on_commit`` (in this case, if a record
          was modified before the ``checker`` starts committing, that record
          will not be committed),
        - an exception was raised during this ``task_execution``.

Providing custom arguments
``````````````````````````

The ``checker`` also allows for arguments to be passed to the checks. This
means that one may write generic checks and write accessible interfaces for
them easily.

- To add arguments to your ``check file`` simply add an ``argument_schema``
  global name in it like in the following example:

    .. code-block:: python

        argument_schema = {
            'field_name': {'type': 'string', 'label': 'New Record Title'},
            'new_number_for_field': {'type': 'float'},
        }

        def check_add_number_to_dynamic_field(record, arguments):
            try:
                current_value = record[arguments['field_name']]
            except KeyError:
                current_value = 0
            record[arguments['field_name']] = arguments['new_number_for_field'] + current_value


    - For the ``type`` key the following types are currently supported:

        - ``string``
        - ``text``
        - ``integer``
        - ``float``
        - ``decimal``
        - ``boolean``
        - ``choice``
        - ``choice_multi``
        - ``datetime``
        - ``date``

        The web interface automatically renders and validates these types.

    - The optional ``label`` key can be any human-friendly description of the
      key.

Providing performance and modification hints
````````````````````````````````````````````

    Because the ``checker`` may be ran against thousands or millions of
    records per ``task_execution``, record-centric checks (those using the
    fixtures mentioned in `Modifying records`_) may be executed in parallel
    (unless ``allow_chunking`` is disabled).

    To handle parallelism, by default, the checker assumes that each batch will
    only modify records from ``requested_recids`` and ``batches`` that are
    expected to modify the same ``requested_recids`` do not run in parallel.

    Two issues arise:
        - The examples above that use ``search`` and ``get_record`` would easily
          break this promise (since they potentially use records other than
          those in ``requested_recids``).
        - The user may be able to provide better hints at runtime.

    To provide such hints class-style check definitions need to be used:

    .. code-block:: python

        argument_schema = {
            'field_name': {'type': 'string', 'label': 'New Record Title'},
            'new_number_for_field': {'type': 'float'},
        }

        class CheckWhatever(object):

            @staticmethod
            def allowed_paths(arguments):
                return ['/' + arguments['field_name']]

            @staticmethod
            def allowed_recids(arguments, requested_recids, all_recids):
                return requested_recids

            def check_fail(self, record, arguments):
                record[arguments['field_name']] = arguments['new_number_for_field']

    The arguments passed to these ``staticmethods`` are always the ones shown
    above (ie they are _not_ fixtures).

    - ``allowed_paths`` must return a list of ``jsonpointer`` strings with all
      the record paths this chunk will potentially modify.

        Here are the three groups of return values:

            ========================  ===================================
                 Return value               Meaning
            ========================  ===================================
            ``['/foo', '/bar/baz']``  Paths ``'/foo'`` and ``'/bar/baz'``
            ``[]``                    All paths
            ``None``                  No paths
            ========================  ===================================

    - ``allowed_recids`` must return all the record IDs that this chunk will
      potentially modify.

        Here are the three groups of return values:

            ========================  =========================================
                 Return value               Meaning
            ========================  =========================================
            ``{1, 2, 3}``             Record IDs 1, 2 and 3
            ``{}``                    All record IDs (``all_recids`` fixture)
            ``None``                  No record IDs
            ========================  =========================================

    .. note::
        The ``all_recids`` argument/fixture contains all the records IDs found
        in the database during this ``task_execution``'s startup. You should
        never modify records outside of this set if you expect things to work
        correctly.


You may find more examples in ``tests/demo_package/checkerext/checks/``.
