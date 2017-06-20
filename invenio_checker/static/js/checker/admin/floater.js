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

Object.size = function(obj) {
  var size = 0, key;
  for (key in obj) {
    if (obj.hasOwnProperty(key)) size++;
  }
  return size;
};

define(
  [
    "jquery",
    "watable",
    'flight/lib/component',
    'hgn!js/checker/admin/templates/floater_checks',
    'hgn!js/checker/admin/templates/floater_tasks',
  ],
  function(
    $,
    _watable,
    defineComponent,
    tplChecks,
    tplTasks
  ) {

    "use strict";

    return defineComponent(CheckerFloater);

    /**
    * .. js:class:: CheckerFloater()
    *
    * TODO
    *
    * :param string load_url: URL to asynchronously load table rows.
    *
    */
    function CheckerFloater() {

      this.attributes({
        // Selectors
        multiActionSelector: ".table-action-multi",
        multiActionBadgeSelector: ".table-action-multi > .badge",
        singleActionSelector: ".table-action-single",

        taskRunButton: ".task_run",
        taskDeleteButton: ".task_delete",
        taskModifyButton: ".task_modify",
        taskNewWithTaskTemplateButton: ".task_new_with_task_tpl",

        tplTasksId: "#floater-tasks",

        // URLs
        taskRunUrl: "/admin/checker/api/task_run",
        taskDeleteUrl: "/admin/checker/api/task_delete",

        // Data
        table_name: null,
        parent_table: null
      });

      var selectedRows = {};

      this.addItemToFloater = function(ev, data) {
        var item_name = data.name;
        selectedRows[item_name] = true;
        this.updateFloater(Object.size(selectedRows));
      };

      this.removeItemFromFloater = function(ev, data) {
        var item_name = data.name;
        delete selectedRows[item_name];
        this.updateFloater(Object.size(selectedRows));
      };

      this.updateFloater = function(selected_rows_len) {
        this._setBadges(selected_rows_len);
        this._enableDisableButtons(selected_rows_len);
      };

      this._setBadges = function(selected_rows_len) {
        $(this.attr.multiActionBadgeSelector).text(selected_rows_len);
      };

      this._enableDisableButtons = function(selected_rows_len) {
        $(".table-action-btn").addClass("disabled");
        switch (selected_rows_len) {
          case 0:
            break;
          case 1:
            $(this.attr.singleActionSelector).removeClass("disabled");
            /* falls through */
          default:
            $(this.attr.multiActionSelector).removeClass("disabled");
        }
      };

      this.enableButtons = function() {
        var that = this;

        $(this.attr.taskRunButton).off('click').on('click', function(event) {
          var selected_tasks = selectedRows;
          $.ajax({
            type: "GET",
            url: that.attr.taskRunUrl,
            data: {task_names: Object.keys(selectedRows)},
            success: function(data) {
              that.trigger(document, "switchTo", {page_name: 'executions'});
            },
            error: function(data) {
              if (data.readyState === 4 && data.responseJSON !== undefined) {
                that.trigger("#alerts", 'alert', {level: 'danger', content: data.responseJSON.error});
              } else {
                that.trigger("#alerts", 'alert', {level: 'danger', content: 'Failed to start tasks!'});
              }
            }
          });
        });

        $(this.attr.taskDeleteButton).off('click').on('click', function(event) {
          $.ajax({
            type: "GET",
            url: that.attr.taskDeleteUrl,
            data: {task_names: Object.keys(selectedRows)},
            success: function(data) {
              that.trigger(that.attr.parent_table, "update");
              that.trigger("#alerts", 'alert', {level: 'warning', content: 'Tasks deleted!'});
            },
            error: function(data) {
              that.trigger("#alerts", 'alert', {level: 'danger', content: 'Failed to delete tasks!'});
            }
          });
        });

        $(this.attr.taskModifyButton).off('click').on('click', function(event) {
          that.trigger(document, "switchTo",
            {page_name: 'task_modify', inherit: Object.keys(selectedRows)[0]});
        });

        $(this.attr.taskNewWithTaskTemplateButton).off('click').on('click', function(event) {
          that.trigger(document, "switchTo",
            {page_name: 'task_branch', inherit: Object.keys(selectedRows)[0]});
        });

      };

      this.initFloater = function(ev, data) {
        if (this.attr.table_name === 'tasks') {
          // Init
          selectedRows = {};

          if (!$(this.attr.tplTasksId).length) {
            $(this.attr.parent_table + " tfoot .btn-toolbar").append(tplTasks());
          }

          // Update with selected rows
          this.updateFloater(Object.size(selectedRows));
          this.enableButtons();
        }
      };

      this.after('initialize', function() {
        // Listeners
        this.on(this.attr.parent_table, "tableCheckboxSelected", this.addItemToFloater);
        this.on(this.attr.parent_table, "tableCheckboxDeselected", this.removeItemFromFloater);
        this.on(this.attr.parent_table, "tableReady", this.initFloater);

        // Log
        console.log("js/checker/admin/floater loaded");
      });

    }
  });
