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
    "jquery"
  ],
  function($) {

    "use strict";

    $('#load_tasks').on('click', function() {
      loadTable('tasks');
    });

    $('#load_checks').on('click', function() {
      loadTable('checks');
    });

    function getTaskColumns(table_name) {
      return $.ajax({
        type: "POST",
        dataType: "json",
        url: "/admin/checker/api/" + table_name + "/get/header"
      });
    }

    function getTaskRows(table_name) {
      return $.ajax({
        type: "POST",
        url: "/admin/checker/api/" + table_name + "/get/data",
      });
    }

    function loadTable(table_name) {
      $.when(getTaskRows(table_name), getTaskColumns(table_name)).done(function(rows, cols) {
        renderTable(rows[0].rows, cols[0].cols);
      });
    }

    function renderTable(rows, cols) {
      $('div#table-container').empty();
      $('div#table-container').WATable({
        data: {
          rows: rows,
          cols: cols
        },
        dataBind: true,
        filter: true,
        types: {
          string: {
          },
          number: {
            decimals: 1
          },
          date: {
            utc: true,
            datePicker: true
          }
        }
      });
    }
  }
);
