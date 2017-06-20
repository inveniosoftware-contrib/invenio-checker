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
    'hgn!js/checker/admin/templates/alerter',
  ],
  function(
    $,
    defineComponent,
    alerter
  ) {

    "use strict";

    return defineComponent(CheckerAlerter);

    /**
    * .. js:class:: CheckerAlerter()
    *
    * TODO
    *
    * :param string load_url: URL to asynchronously load table rows.
    *
    */
    function CheckerAlerter() {

      this.attributes({
        // Selectors

        // URLs

        // Data
      });

      this.tblAlert = function (ev, data) {
        $("#alerts").append(alerter({level: data.level, content: data.content,
        content_pre: data.content_pre}));
      };

      this.alertClear = function(ev) {
        $("#alerts").empty();
      };

      this.after('initialize', function() {
        // Listeners
        this.on("#alerts", "alert", this.tblAlert);
        this.on("#alerts", "clear", this.alertClear);

        // Log
        console.log("js/checker/admin/alerter loaded");
      });

    }
  }
);
