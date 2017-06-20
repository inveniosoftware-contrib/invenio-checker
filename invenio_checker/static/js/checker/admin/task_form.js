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

define(
  [
    "jquery",
    "watable",
    'flight/lib/component',
  ],
  function(
    $,
    _watable,
    defineComponent
  ) {

    "use strict";

    return defineComponent(CheckerSubmissionForm);

    /**
    * .. js:class:: CheckerSubmissionForm()
    *
    * TODO
    *
    * :param string load_url: URL to asynchronously load table rows.
    *
    */
    function CheckerSubmissionForm() {

      this.attributes({
        // Selectors
        scheduleSelector: '#schedule',

        // URLs
        newTaskFormUrl: '/admin/checker/render/new_task_form',

        // Data
        cronExpDiv: "<div id='cronexp' style='display: inline;'></div>",
      });

      // this.plugTypingSearchPattern = function() {
      //   $("#filter_pattern").on('keyup', function() {
      //     $.get(
      //       "/admin/checker/api/records/get",
      //       {query: $(this).val()},
      //       function(data) {
      //         $('#matching-records').empty();
      //         $('#matching-records').append(data);
      //       }
      //     );
      //   });
      // };

      this._onFormFilled = function(inherit) {
        // If the form filler changed the `schedule`, we need to tell jqcron
        $('#cronexp').jqCronGetInstance().setCron($("#schedule").val());
        this.trigger("#new-task-form", "submissionFormFieldsReady", {for_rule: inherit});
        $("#creation").show();
      };

      this._attachJqCron = function() {
        $("#schedule_enabled").after(this.attr.cronExpDiv);
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
          bind_to: $(this.attr.scheduleSelector),
          bind_method: {
            set: function($element, value) {
              $element.val(value);
            }
          }
        });
        $("#cronexp").hide();
      };

      // FIXME: Move to different file
      // this.updateFormTitle = function() {
      //   Update title of form
      //   var action_to_human = {
      //     task_create: "Please enter the new task's details",
      //     task_modify: "Please modify the task",
      //   };
      //   $.ajax({
      //     type: "GET",
      //     url: "/admin/checker/translate",
      //     data: {english: action_to_human[action]},
      //     success: function(data) {
      //       $("#creation .panel-title").text(data);
      //     }
      //   });
      // };

      this._periodicToggle = function(is) {
        // Show and hide buttons depending on whether `periodic` is checked
        $("#schedule_enabled").attr('checked', is);
        $(".button_when_periodic").prop("disabled", !is);
        $(".button_when_not_periodic").prop("disabled", is);
        if (is) {
          $("#cronexp").show();
        } else {
          $("#cronexp").hide();
        }
      };

      this.hideForm = function() {
        $("#new-task-form").empty();
      };

      this._fillForm = function(inherit, callback) {
        $.ajax({
          type: "POST",
          url: "/admin/checker/api/tasks/get/data/" + inherit,
          success: function(template) {

            for (var key in template) {
              if (template.hasOwnProperty(key)) {
                var elem = $("#creation").find("#"+key);
                var new_value = template[key];
                if ($(elem).is(":checkbox")) {
                  if (new_value === 1) {
                    $(elem).click();
                  }
                } else {
                  $(elem).val(new_value);
                }
              }
            }
            callback(inherit);
          }
        });
      };

      this.renderTaskForm = function(ev, data) {
        var inherit = data.inherit;
        var action = data.page_name;
        var that = this;

        $("#new-task-form").load(this.attr.newTaskFormUrl, function () {
          // Set metadata fields and hide them
          if (action === 'task_modify') {
            $("#modify").prop("checked", true);
            $("#original_name").val(inherit);
          } else {
            $("#modify").prop("checked", false);
          }
          $("#requested_action, #schedule, #modify, #original_name")
          .closest(".row").hide();

          // Enable dynamic subform loading
          $("#plugin").on('change', function() {
            that.trigger("#new-task-form", "updatePluginArguments",
            {original_name: $("#original_name").val()});
          });
          $("#reporters").on('change', function(event) {
            that.trigger("#new-task-form", "updateReporterArguments",
            {original_name: $("#original_name").val()});
          });

          that.trigger("#alerts", "clear");

          // Prepare inputs for scheduling
          that._attachJqCron();
          $('#schedule_enabled').bind('change', function(e) {
            that._periodicToggle(this.checked);
          });
          that._periodicToggle($('#schedule_enabled').prop('checked'));

          // Fill in fields that exist in template
          if (inherit !== undefined) {
            that._fillForm(inherit, that._onFormFilled.bind(that));
          } else {
            that._onFormFilled(inherit);
          }

        });
      };

      this.after('initialize', function() {
        // Listeners
        this.on("#creation", "load", this.renderTaskForm);
        this.on("#creation", "hide", this.hideForm);

        // Log
        console.log("js/checker/admin/task_form loaded");
      });

    }

  }
);
