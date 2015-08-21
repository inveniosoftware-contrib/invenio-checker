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

      var table_data = {
        otherStuff: {}
      };

      $(document).ready(function () {
        getTaskColumns();
        getTaskRows();
      });

      function getTaskColumns() {
        $.ajax({
          type: "POST",
          dataType: "json",
          url: "/admin/checker/api/tasks/get/header",
          success: function(result) {
            table_data.cols = result.columns;
          }
        });
      }

      function getTaskRows() {
        $.ajax({
          type: "POST",
          url: "/admin/checker/api/tasks/get/data",
          success: function(result) {
            table_data.rows = result.rows;

            //First example. No options supplied.
            $('div#table-container').WATable({
              data: table_data,
              filter: true,
              types: {
                // If you want, you can supply some properties that will be
                // applied for specific data types.
                string: {
                  //filterTooltip: "Giggedi..."    //What to say in tooltip when hoovering filter fields. Set false to remove.
                  //placeHolder: "Type here..."    //What to say in placeholder filter fields. Set false for empty.
                },
                number: {
                  decimals: 1   //Sets decimal precision for float types
                },
                bool: {
                  //filterTooltip: false
                },
                date: {
                  utc: true,            //Show time as universal time, ie without timezones.
                  //format: 'yy/dd/MM',   //The format. See all possible formats here http://arshaw.com/xdate/#Formatting.
                  datePicker: true      //Requires "Datepicker for Bootstrap" plugin (http://www.eyecon.ro/bootstrap-datepicker).
                }
              }
            });

            $(".watable.table").css("width", "100%");

          }
        });
      }

      console.info("js/checker/admin/watable is loaded")
    }


);
