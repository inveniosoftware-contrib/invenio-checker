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

    return defineComponent(CheckerSubmissionFiller);

    /**
    * .. js:class:: CheckerSubmissionFiller()
    *
    * TODO
    *
    * :param string load_url: URL to asynchronously load table rows.
    *
    */
    function CheckerSubmissionFiller() {

      this.attributes({
        // Selectors

        // URLs

        // Data
        // table_name: null,
        // parent_table: null
      });

      var last_pressed_button = null;

      this.updateAllArguments = function(ev, data) {
        this.updatePluginArguments(ev, data);
        this.updateReporterArguments(ev, data);
      };

      this._valueOrNull = function(value){
        if (!Boolean(value)) {
          return null;
        }
        return value;
      };

      this.updatePluginArguments = function(ev, data) {
        var that = this;
        var for_rule = this._valueOrNull(data.for_rule);
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
              that._plugDatePickers();
            }
          }
        });
      };

      this.updateReporterArguments = function(ev, data) {
        var that = this;
        var for_rule = this._valueOrNull(data.for_rule);
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
                  that._plugDatePickers();
                }
              }
            });
          }
        });
      };

      this._plugDatePickers = function() {
        // Call this every time you load objects that may contain datepickers.
        $('.datetimepicker').datetimepicker({
          format: 'YYYY-MM-DD HH:mm:ss'
        });
        $('.datepicker').datetimepicker({
          format: 'YYYY-MM-DD'
        });
      };

      this.after('initialize', function() {
        // Listeners
        var node_id = "#" + this.node.id;
        this.on(node_id, "updatePluginArguments", this.updatePluginArguments);
        this.on(node_id, "updateReporterArguments", this.updateReporterArguments);
        this.on(node_id, "updateAllArguments", this.updateAllArguments);
        this.on(node_id, "submissionFormFieldsReady", this.updateAllArguments);

        // Log
        console.log("js/checker/admin/task_form_filler loaded");
      });

    }
  }
);
