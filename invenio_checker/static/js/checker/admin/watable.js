/*
* This file is part of Invenio.
* Copyright (C) 2015 CERN.
*
* Invenio is free software; you can redistribute it and/or
* modify it under the terms of the GNU General Public License as
* published by the Free Software Foundation; either version 2 of the
* License, or (at your option) any later version.
*
* Invenio is distributed in the hope that it will be useful, but
* WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
* General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with Invenio; if not, write to the Free Software Foundation, Inc.,
* 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.
*/

var dbg = "";

define(
  [
    "jquery",
  ],
  function($) {

    "use strict";

    var current_page = "";
    var last_pressed_button = "";
    var tbl = null;
    var floater = null;

    // Default page
    $(document).ready(function () {
      floater = $("#side-floater");
      plugJqueryForms();
      plugDatePickers();
      switchTo(requested_page);
      renderPeriodic();
    });

    // Page switching
    $('#load_tasks, #load_checks, #load_logs, #create_task')
    .on('click', function(event) {
      function id_to_url (id_) {
        return $("#" + id_).attr('href');
      }
      function id_to_page_name (id_) {
        return {
          load_tasks : 'tasks',
          load_checks : 'checks',
          load_logs : 'executions',
          create_task : 'create_task'
        }[id_];
      }
      event.preventDefault();
      var id_ = event.target.id;
      history.pushState('data', '', id_to_url(id_));
      switchTo(id_to_page_name(id_));
    });

    function switchTo(page_name) {
      $(".switchable").hide();
      updateSubtitle(page_name);
      if (page_name === 'create_task') {
        createNewTask();
      }
      else {
        loadTable(page_name);
      }
    }

    // Task creation
    function plugDatePickers() {
      $('.datetimepicker').datetimepicker({
        format: 'YYYY-MM-DD HH:mm:ss'
      });
      $('.datepicker').datetimepicker({
        format: 'YYYY-MM-DD'
      });
    }

    function plugJqueryForms() {

      function handleResponse(response, statusText, xhr, $form) {
        if (response.success === false) {
          switch (response.failure_type) {
            case 'validation':
              $(".validation-error").html(function() {
                var field_id = $(this).data('field-id');
                if (field_id in response.errors) {
                  $(this).html(response.errors[field_id]);
                  $(this).show();
                }
                else {
                  $(this).hide();
                }
              });
              break;
            case 'general':
              $("#task-insertion-failure").html('<strong>Failed to complete request:</strong> ' + response.errors);
              $("#task-insertion-failure").show();
              break;
            default:
              $("#task-insertion-failure").
              text('<strong>Bad reply:</strong> Missing failure type');
              $("#task-insertion-failure").show();
          }
        }
        else {
          // TODO: Forward to newly created rule
        }
      }

      function beforeSubmit(formData, jqForm, options) {
        $("#task-insertion-failure").hide();
        $(".validation-error").html('');
        if (null === last_pressed_button) {
          // No button was explicitly clicked. Bail out.
          return false;
        }
        jqForm[0].requested_action.value = last_pressed_button;
      }

      var options = {
        beforeSubmit: beforeSubmit,
        success: handleResponse,
        dataType: 'json',
        resetForm: false,
        clearForm: false
      };

      // Have different buttons add different attributes to the form
      $("[id^='submit_']").on('click', function(event) {
        last_pressed_button=$(this).attr("id");
        return true;
      });
      // bind to the form's submit event
      $('#new_task_form').submit(function() {
          $(this).ajaxSubmit(options);
          return false; // prevent standard browser behaviour
      });

      // Prepare periodic checks
      $("#periodic").after("<div id='cronexp' style='display: inline;'></div>");
      periodicToggle(false);
      $("#cronexp").hide();

      // Prepare requested action
      $("#requested_action").closest(".row").hide();

      // Hide previously displayed failure
      $("#task-insertion-failure").hide();

    }

    function createNewTask(check_blueprint, task_blueprint) {
      if (check_blueprint !== undefined) {
        // TODO: Load blueprint
      }
      else if (task_blueprint) {
        // TODO: Load blueprint
      }
      updateCreationArguments();
      $("#schedule").closest(".row").css("display", "none");
      $("#creation").show();
    }

    $("#plugin").change(function() {
      updateCreationArguments();
    });

    // FIXME: Doesn't run on resetForm :<
    $("#new_task_form").on('reset', function() {
      updateCreationArguments();
    });

    function updateCreationArguments() {
      var arg_rows = $("[id^='arg_']").closest(".row");
      var plugin = $("#plugin");
      var plugin_row = $(plugin).closest(".row");
      $.ajax({
        type: "POST",
        dataType: "html",
        url: "/admin/checker/api/create_task/get_arguments_spec/" +
          $(plugin).val(),
        success: function(data) {
          $(arg_rows).remove();
          $(plugin_row).after(data);
          plugDatePickers();
        }
      });
    }

    // Periodic
    $('#periodic').bind('change', function(e) {
      var cur_input = $(this);
      if (cur_input.is(':checked')) {
        periodicToggle(true);
      } else {
        periodicToggle(false);
      }
    });

    function renderPeriodic() {
      $('#cronexp').jqCron({
        enabled_minute: true,
        multiple_dom: true,
        multiple_month: true,
        multiple_mins: true,
        multiple_dow: true,
        multiple_time_hours: true,
        multiple_time_minutes: true,
        default_period: 'month',
        default_value: '0 0 1 * *',
        no_reset_button: false,
        lang: 'en',
        bind_to: $('#schedule'),
        bind_method: {
          set: function($element, value) {
            $element.val(value);
          }
        }
      });
    }

    function periodicToggle(is){
      $("#periodic").attr('checked', is);
      $(".button_when_periodic").prop("disabled", !is);
      $(".button_when_not_periodic").prop("disabled", is);
      if (is) {
        $("#cronexp").show();
      } else {
        $("#cronexp").hide();
      }
    }

    // Display tables
    function loadTable(table_name) {
      $('div#table-container').url = '/admin/checker/api/'+table_name+'/get/data';
      if (tbl === null) {
        tbl = $('div#table-container').WATable({
          url: '/admin/checker/api/'+table_name+'/get/data',
          pageSize: 30,
          preFill: false,
          filter: true,
          hidePagerOnEmpty: true,
          checkboxes: true,
          columnPicker: true,
          types: {
            string: {
              placeHolder: "Filter"
            },
            number: {
              decimals: 2
            },
            date: {
              utc: false,
              datePicker: false
            }
          },
          rowClicked: function(data) {
            if (table_name === 'executions') {
              data.event.preventDefault();
              showLog(data.row.uuid);
            }
            else if (table_name === 'checks') {
              data.event.preventDefault();
              showFile(data.row.name);
            }
          },
          tableCreated: function(data) {
            bindCheckboxes(table_name);
            refreshFloater(table_name);
          }
        }).data('WATable');
      } else {
       tbl.option('url', '/admin/checker/api/'+table_name+'/get/data');
       tbl.update(function() {
         bindCheckboxes(table_name);
         refreshFloater(table_name);
       });
      }
      $('div#table-container').show();
    }

    function bindCheckboxes(table_name) {
      $('.watable-col-cbunique :checkbox, #table-container .checkToggle').on('change', function() {
        refreshFloater(table_name);
      });
    }

    function refreshFloater(table_name){
      // Reset
      $("#side-floater").hide();  // hide is faster than remove, removes flicker

      // Don't rely on WATable's .getData(true) to get checked rows as it is
      // not updated mid-check.
      var selected_rows_len = $(".watable-col-cbunique :checkbox:checked").length;

      if (table_name === 'tasks') {
        $("#table-container tfoot .btn-toolbar").append(floater);

        // Prepare
        $(".table-action-btn").addClass("disabled");
        $(".table-action-multi > .badge").text(selected_rows_len);
        // Enable supported actions
        switch (selected_rows_len) {
          case 0:
            break;
          case 1:
            $(".table-action-single").removeClass("disabled");
          default:
            $(".table-action-multi").removeClass("disabled");
        }
        $("#side-floater").show();
      }
    }

    // Components and state
    function updateSubtitle(page_name) {
      current_page = page_name;
      var subtitles = {
        tasks: 'Tasks view',
        checks: 'Checks view',
        executions: 'Executions view',
        create_task: 'Create task',
      };
      var subtitle_en = subtitles[page_name];
      $.ajax({
        type: "GET",
        url: "/admin/checker/translate",
        data: {english: subtitle_en},
        success: function(data) {
          $('#subtitle').text(data);
        }
      });
    }

    // Modals
    function showLog(uuid) {
      $.ajax({
        type: "GET",
        url: '/admin/checker/api/executions/stream_structured/'+uuid,
        success: function(data) {
          $('#dialogModal .modal-body').text(data);
          $('#dialogModal').modal('show');
        }
      });
    }

    function showFile(uuid) {
      $.ajax({
        type: "GET",
        url: '/admin/checker/api/checks/stream_check/'+uuid,
        success: function(data) {
          $('#dialogModal .modal-body').text(data);
          $('#dialogModal .modal-title').text('');
          $('#dialogModal').modal('show');
        }
      });
    }

  }
);
