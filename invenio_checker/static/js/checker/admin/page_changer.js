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
    'flight/lib/component',
    'hgn!js/checker/admin/templates/scheduler_disabled_warning'
  ],
  function(
    $,
  defineComponent,
  schedulerDisabledWarning) {

    "use strict";

    return defineComponent(CheckerPageChanger);

    /**
    * .. js:class:: CheckerPageChanger()
    *
    * TODO
    *
    * :param string load_url: URL to asynchronously load table rows.
    *
    */
    function CheckerPageChanger() {

      this.attributes({
        // Selectors
        switchableSelector: ".switchable",

        // URLs

        // Data
      });

      this.warnIfSchedulerDisabled = function() {
      /*
       * Call this only once.
       */
        $.ajax({
          type: "GET",
          url: "/admin/checker/scheduler_is_enabled",
          success: function(data) {
            if (data.enabled === false) {
              $("#header-warnings").append(schedulerDisabledWarning());
            }
          }
        });
      };

      this.switchTo = function (ev, data) {
        /**
        * :param data:
        * {
        *   page_name: one-of(tasks, checks, executions, task_create, task_modify)
        *   inherit (if page_name in (task_create, task_modify)): {task_name: string, plugin: string}
        * }
        */

        var page_name = data.page_name;
        var inherit = data.inherit;

        history.pushState('data', '', page_name);
        this._updateSubtitle(page_name);

        this.trigger("#alerts", 'alert_clear');
        switch(page_name) {
          case 'task_create':
          case 'task_modify':
          case 'task_branch':
            this.trigger(this.attr.switchableSelector+":not(#creation)", "hide");
            this.trigger("#creation", "load", {page_name: page_name, inherit: inherit});
            break;
          case 'tasks':
          case 'checks':
          case 'executions':
            this.trigger(this.attr.switchableSelector+":not(#table-container-"+page_name+")", "hide");
            this.trigger("#table-container-"+page_name, "load");
            break;
        }

      };

      // Components and state
      this._updateSubtitle = function(page_name) {
        var subtitles = {
          tasks: 'Tasks',
          checks: 'Checks',
          executions: 'Executions',
          task_create: 'Create task',
          task_modify: 'Modify task',
          task_branch: 'Create task',
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
      };

      this.after('initialize', function() {
        this.on(document, "switchTo", this.switchTo);
        this.warnIfSchedulerDisabled();
        console.log("js/checker/admin/page_changer loaded");
      });

    }

  });
