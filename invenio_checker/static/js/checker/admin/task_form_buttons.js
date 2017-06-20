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

    return defineComponent(CheckerSubmissionButtons);

    /**
    * .. js:class:: CheckerSubmissionButtons()
    *
    * TODO
    *
    * :param string load_url: URL to asynchronously load table rows.
    *
    */
    function CheckerSubmissionButtons() {

      this.attributes({
        // Selectors

        // URLs

        // Data
        // table_name: null,
        // parent_table: null
        last_pressed_button: "",
      });

      this.plugFormButtons = function(ev) {
        var that = this;
        var options = {
          beforeSubmit: function (formData, jqForm, options) {
            that.trigger("#alerts", "clear");
            $(".validation-error").hide();
            if (that.attr.last_pressed_button === "") {
              // No button was explicitly clicked. Bail out.
              return false;
            }
            formData.map(function(item) {
              if (item.name === 'requested_action') {
                item.value = that.attr.last_pressed_button;
              }
            });
          },
          success: function (response, statusText, xhr, $form) {
            switch (that.attr.last_pressed_button) {
              case 'submit_save':
              case 'submit_schedule':
                that.trigger(document, "switchTo", {page_name: "tasks"});
                break;
              case 'submit_run_and_schedule':
              case 'submit_run':
                that.trigger(document, "switchTo", {page_name: "executions"});
                break;
            }
          },
          error: function (xhr, whatHappen, statusText, $form) {
            if (xhr.readyState === 4) {
              var response = xhr.responseJSON;
              switch (response.failure_type) {
                case 'validation':
                  // Go over all the validation fields Show the ones that have
                  // errors, hide the ones that don't.
                  $(".validation-error").html(function() {
                    var field_id = $(this).data('field-id');
                    if (field_id in response.errors) {
                      $(this).html(response.errors[field_id]).show();
                    }
                    else {
                      $(this).hide();
                    }
                  });
                    that.trigger("#alerts", "alert",
                    {level: 'danger', content: 'Form does not pass validation. Please look above for specific errors.'});
                  break;
                case 'general':
                  that.trigger("#alerts", "alert",
                  {level: 'danger', content: 'Failed to complete request', content_pre: response.errors});
                  break;
                default:
                  that.trigger("#alerts", "alert",
                  {level: 'danger', content: 'Bad reply from server: Missing failure type'});
              }
            } else {
              that.trigger(document, "alert",
              {level: 'danger', content: 'Unknown error while submitting task, sorry!'});
            }
          },
          dataType: 'json',
          resetForm: false,
          clearForm: false
        };

        // Have different form buttons do different actions
        $("[id^='submit_']").on('click', function(event) {
          that.attr.last_pressed_button=$(this).attr("id");
          return true;
        });

        // bind to the form's submit event
        $('#new_task_form').submit(function() {
          $(".tbl_alert.alert-danger").hide();
          $(this).ajaxSubmit(options);
          return false; // prevent standard browser behaviour
        });
      };

      this.after('initialize', function() {
        // Listeners
        this.on("#new-task-form", "submissionFormFieldsReady", this.plugFormButtons);

        // Log
        console.log("js/checker/admin/task_form_buttons loaded");
      });

    }
  });
