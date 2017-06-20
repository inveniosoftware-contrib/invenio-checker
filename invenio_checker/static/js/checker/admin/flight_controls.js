/*
 * This file is part of Invenio.
 * Copyright (C) 2014, 2015 CERN.
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
    'jquery',

    "js/checker/admin/page_changer",
    "js/checker/admin/main_menu",

    "js/checker/admin/table",
    "js/checker/admin/floater",

    "js/checker/admin/task_form",
    "js/checker/admin/task_form_buttons",
    "js/checker/admin/task_form_filler",

    "js/checker/admin/alerter",
  ],
  function(
    $,

    CheckerPageChanger,
    CheckerMainMenu,

    CheckerTable,
    CheckerTableButtons,

    CheckerSubmissionForm,
    CheckerSubmissionButtons,
    CheckerSubmissionFiller,

    CheckerAlerter
  ) {

    "use strict";

    function initialize(context) {

      CheckerPageChanger.attachTo(document);
      CheckerMainMenu.attachTo("#checker-navbar");

      CheckerTable.attachTo("#table-container-checks", {
        table_name: "checks",
      });
      CheckerTable.attachTo("#table-container-tasks", {
        table_name: "tasks",
      });
      CheckerTable.attachTo("#table-container-executions", {
        table_name: "executions",
      });

      CheckerTableButtons.attachTo("#table-container-checks", {
        table_name: "checks",
        parent_table: "#table-container-checks",
      });
      CheckerTableButtons.attachTo("#table-container-tasks", {
        table_name: "tasks",
        parent_table: "#table-container-tasks",
      });

      CheckerSubmissionForm.attachTo("#creation");
      CheckerSubmissionButtons.attachTo("#creation");
      CheckerSubmissionFiller.attachTo("#creation");

      CheckerAlerter.attachTo(document);
    }

    return initialize;
  }
);
