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
    var tbl_alert = null;

    // Default page
    $(document).ready(function () {
      floater = $("#side-floater");
      tbl_alert = $(".tbl_alert");
      enablePageSwitching();
      plugFormSubmission();
      switchTo(requested_page);
    });

    function enablePageSwitching() {
      $('#load_tasks, #load_checks, #load_logs, #task_create')
      .on('click', function(event) {
        function id_to_url (id_) {
          return $("#" + id_).attr('href');
        }
        event.preventDefault();
        function id_to_page_name (id_) {
          return {
            load_tasks : 'tasks',
            load_checks : 'checks',
            load_logs : 'executions',
            task_create : 'task_create',
          }[id_];
        }
        var id_ = event.target.id;
        switchTo(id_to_page_name(id_));
      });
    }

    function switchTo(page_name, inherit) {
      history.pushState('data', '', page_name);
      $(".switchable").hide();
      updateSubtitle(page_name);
      refreshFloater();
      if (['task_create', 'task_modify'].indexOf(page_name) >= 0) {
        renderTaskForm(page_name, inherit);
      } else {
        loadTable(page_name);
      }
    }

    function plugDatePickers() {
      $('.datetimepicker').datetimepicker({
        format: 'YYYY-MM-DD HH:mm:ss'
      });
      $('.datepicker').datetimepicker({
        format: 'YYYY-MM-DD'
      });
    }

    function plugFormSubmission() {

      function handleError(xhr, whatHappen, statusText, $form) {
        if (xhr.readyState === 4) {
          var response = xhr.responseJSON;
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
        } else {
              tblAlert('danger', 'Unknown error occurred while submitting task!');
        }
      }

      function handleResponse(response, statusText, xhr, $form) {
          // TODO: Forward to newly created rule
      }

      function beforeSubmit(formData, jqForm, options) {
        $("#task-insertion-failure").hide();
        $(".validation-error").hide();
        if (null === last_pressed_button) {
          // No button was explicitly clicked. Bail out.
          return false;
        }
        formData.map(function(item) {
          if (item.name === 'requested_action') {
            item.value = last_pressed_button;
          }
        });
      }

      var options = {
        beforeSubmit: beforeSubmit,
        success: handleResponse,
        error: handleError,
        dataType: 'json',
        resetForm: false,
        clearForm: false
      };

      // Have different buttons do different actions
      $("[id^='submit_']").off('click').on('click', function(event) {
        last_pressed_button=$(this).attr("id");
        return true;
      });
      // bind to the form's submit event
      $('#new_task_form').submit(function() {
          $(".tbl_alert.alert-danger").hide();
          $(this).ajaxSubmit(options);
          return false; // prevent standard browser behaviour
      });
    }

    function formFilled(action, inherit) {
      plugDatePickers();
      plugTypingSearchPattern();
      // Refresh refreshable javascript elements
      if (inherit === undefined) {
        updateAllArguments();
      } else {
        updateAllArguments($("#name").val());
      }
      $("#creation").show();
    }

    function renderTaskForm(action, inherit) {

      // Update title of form
      // var action_to_human = {
      //   task_create: "Please enter the new task's details",
      //   task_modify: "Please modify the task",
      // };
      // $.ajax({
      //   type: "GET",
      //   url: "/admin/checker/translate",
      //   data: {english: action_to_human[action]},
      //   success: function(data) {
      //     $("#creation .panel-title").text(data);
      //   }
      // });

      // Make sure we start with no arguments
      $(".plugin-args .reporter-args").remove();

      // Set metadata fields and hide them
      if (action === 'task_modify') {
        $("#modify").prop("checked", true);
        $("#original_name").val($("#name").val());
      } else {
        $("#modify").prop("checked", false);
      }
      $("#requested_action, #schedule, #modify, #original_name").closest(".row").hide();

      // Enable subform loading
      $("#plugin").off('change').on('change', function() {
        updatePluginArguments($("#original_name").val());
      });
      $("#reporters").off('change').on('change', function(event) {
        updateReporterArguments($("#original_name").val());
        // event.preventDefault();
      });

      // Hide previously displayed failure
      $("#task-insertion-failure").hide();

      // Prepare inputs for periodic
      attachJqCron(false);
      periodicToggle(false);
      $('#schedule_enabled').bind('change', function(e) {
        console.log(this.checked);
        periodicToggle(this.checked);
      });

      // Reset all fields
      // $("#new_task_form")[0].reset();  // Skips nested fields, so we do:
      $("#new_task_form input[type!=checkbox]").val('');
      $("#new_task_form input[type=checkbox]").prop('checked', false);
      $("#creation").show();

      // Fill in fields that exist in template
      if (inherit !== undefined) {
        $.ajax({
          type: "POST",
          url: "/admin/checker/api/tasks/get/data/" + inherit.task_name,
          success: function(template) {

            $("[id^='arg_']").closest(".row").remove();
            for (var key in template) {
              if (template.hasOwnProperty(key)) {
                var elem = $("#creation").find("#"+key);
                var new_value = template[key];
                if ($(elem).is(":checkbox")) {
                  if (new_value === true) {
                    // We've already made sure it's not clicked.
                    $(elem).click();
                  }
                } else {
                  $(elem).val(new_value);
                }
              }
            }
            formFilled(action, inherit);

          }
        });
      } else {
        formFilled(action, inherit);
      }

    }

    function plugTypingSearchPattern() {  // XXX Why is this called twice?
      $("#filter_pattern").off('keyup').on('keyup', function() {
        $.get(
          "/admin/checker/api/records/get",
          {query: $(this).val()},
          function(data) {
            $('#matching-records').empty();
            $('#matching-records').append(data);
          }
        );
      });
    }

    function updateAllArguments(for_rule) {
      updatePluginArguments(for_rule);
      updateReporterArguments(for_rule);
    }

    function updatePluginArguments(for_rule) {
      var plugin = $("#plugin");
      var plugin_row = $(plugin).closest(".row");
      var plugin_name = $(plugin).val();
      $.ajax({
        type: "POST",
        url: "/admin/checker/api/task_create/get_arguments_spec/",
        data: {plugin_name: plugin_name, task_name: for_rule},
        success: function(data) {
          $(".plugin-args").remove();
          if (data.trim()) {
            $(plugin_row).after('<div class="plugin-args well well-sm">'+data+'</div>');
            plugDatePickers();
          }
        }
      });
    }

    function updateReporterArguments(for_rule) {
      var reporters_row = $("#reporters").closest(".row");
      $("#reporters :not(option:selected)").map(function(i, el) {
        $("[id^='arg_"+el.value+"']").closest(".reporter-args").remove();
      });
      $("#reporters option:selected").map(function(i, el) {
        var selector = "[id^='arg_"+el.value+"']";
        if ($(selector).length === 0) {
          $.ajax({
            type: "POST",
            url: "/admin/checker/api/task_create/get_arguments_spec/",
            data: {plugin_name: el.value, task_name: for_rule},
            success: function(data) {
              if (data.trim()) {
                $(reporters_row).after('<div class="reporter-args well well-sm">'+data+'</div>');
                plugDatePickers();
              }
            }
          });
        }
      });
    }

    function attachJqCron(show) {
      $("#schedule_enabled").after("<div id='cronexp' style='display: inline;'></div>");
      if (!show) {
        $("#cronexp").hide();
      }
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
      $("#schedule_enabled").attr('checked', is);
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
            // `table_name` is not injected from the `else` block below. this
            // is why we use `current_page` here.
            if (current_page === 'executions') {
              data.event.preventDefault();
              showLog(data.row.uuid);
            }
            else if (current_page === 'checks') {
              data.event.preventDefault();
              showFile(data.row.name);
            }
          },
          tableCreated: function(data) {
            bindCheckboxes(table_name);
            refreshFloater();
          }
        }).data('WATable');
      } else {
       tbl.option('url', '/admin/checker/api/'+table_name+'/get/data');
       tbl.update(function() {
         bindCheckboxes(table_name);
         refreshFloater();
       });
      }
      $('div#table-container').show();
    }

    function bindCheckboxes(table_name) {
      $('.watable-col-cbunique :checkbox, #table-container .checkToggle').off('change').on('change', function() {
        refreshFloater();
      });
    }

    function refreshFloater() {
      // Reset
      $(floater).hide();  // hide is faster than remove, removes flicker

      // Don't rely on WATable's .getData(true) to get checked rows as it is
      // not updated mid-check.
      var selected_rows_len = $(".watable-col-cbunique :checkbox:checked").length;

      if (current_page === 'tasks') {
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
            // No, no break statement with my coffee, thank you.
          default:
            $(".table-action-multi").removeClass("disabled");
        }

        $(".task_run").off('click').on('click', function(event) {
          var selected_tasks = [];
          $.each(tbl.getData(true).rows, function(idx, row) {
            selected_tasks.push(row.name);
          });
          $.ajax({
            type: "GET",
            url: "/admin/checker/task_run",
            data: {task_names: selected_tasks},
            success: function(data) {
              tblAlert('success', 'Tasks started!');
            },
            error: function(data) {
              if (data.readyState === 4 && data.responseJSON !== undefined) {
                tblAlert('danger', data.responseJSON.error);
              } else {
                tblAlert('danger', 'Could not start tasks!');
              }
            }
          });
        });

        $(".task_delete").off('click').on('click', function(event) {
          var selected_tasks = [];
          $.each(tbl.getData(true).rows, function(idx, row) {
            selected_tasks.push(row.name);
          });
          $.ajax({
            type: "GET",
            url: "/admin/checker/task_delete",
            data: {task_names: selected_tasks},
            success: function(data) {
              tbl.update();
              tblAlert('warning', 'Tasks deleted!');
            },
            error: function(data) {
              tblAlert('danger', 'Failed to delete tasks!');
            }
          });
        });

        $(".task_modify").off('click').on('click', function(event) {
          var inherit = {};
          $.each(tbl.getData(true).rows, function(idx, row) {
            // Only one row is selected anyway
            inherit.task_name = row.name;
            inherit.plugin = row.plugin;
          });
          switchTo('task_modify', inherit);
        });

        $(".task_new_with_task_tpl").off('click').on('click', function(event) {
          var inherit = {};
          $.each(tbl.getData(true).rows, function(idx, row) {
            // Only one row is selected anyway
            inherit.task_name = row.name;
            inherit.plugin = row.plugin;
          });
          switchTo('task_create', inherit);
        });

        $(floater).show();
      }
    }

    function tblAlert(level, content) {
      var new_alert = tbl_alert.clone();
      $(new_alert).find('p').text(content);
      $(new_alert).addClass("alert-"+level);
      $(new_alert).hide().appendTo("#table-container").toggle('highlight');
    }

    // Components and state
    function updateSubtitle(page_name) {
      current_page = page_name;
      var subtitles = {
        tasks: 'Tasks view',
        checks: 'Checks view',
        executions: 'Executions view',
        task_create: 'Create task',
        task_modify: 'Modify task',
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
