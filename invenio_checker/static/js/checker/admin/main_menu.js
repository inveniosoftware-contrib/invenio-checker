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
    'flight/lib/component'
  ],
  function(
    $,
  defineComponent) {

    "use strict";

    return defineComponent(CheckerMainMenu);

    /**
    * .. js:class:: CheckerMainMenu()
    *
    * TODO
    *
    * :param string load_url: URL to asynchronously load table rows.
    *
    */
    function CheckerMainMenu() {

      this.attributes({
        // Selectors

        // URLs

        // Data
        // current_page: null,
      });

      this.enablePageSwitching = function(ev, data){
        var that = this;
        $('#load_tasks, #load_checks, #load_logs, #task_create')
        .on('click', function(event) {
          event.preventDefault();
          var id_to_page_name = {
            load_tasks : 'tasks',
            load_checks : 'checks',
            load_logs : 'executions',
            task_create : 'task_create',
          };
          that.trigger(document, "switchTo", {page_name: id_to_page_name[event.target.id]});
        });
      };

      this.after('initialize', function() {
        this.enablePageSwitching();
        console.log("js/checker/admin/main_menu loaded");
      });

    }

});
