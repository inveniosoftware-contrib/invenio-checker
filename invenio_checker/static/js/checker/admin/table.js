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
    'flight/lib/component'
  ],
  function(
    $,
    _watable,
    defineComponent
  ) {

    "use strict";

    return defineComponent(CheckerTable);

    /**
    * .. js:class:: CheckerTable()
    *
    * TODO
    *
    * :param string load_url: URL to asynchronously load table rows.
    *
    */
    function CheckerTable() {

      this.attributes({
        // Selectors

        // URLs
        getTableDataUrl: '/admin/checker/api/{0}/get/data',

        // Data
        table_ref: {},
        table_name: null,
      });

      this.hideTable = function(ev) {
        $("#"+this.node.id).hide();
      };

      this.updateTable = function(ev) {
        this.attr.table_ref.update();
      };

      // Display tables
      this.loadTable = function(ev) {
        /*
        */
        this.attr.table_id = "#" + this.node.id;
        var getDataUrl = this.attr.getTableDataUrl.format(this.attr.table_name);
        var that = this;

        if ($.isEmptyObject(this.attr.table_ref)) {
          this.attr.table_ref = $(this.attr.table_id).WATable({
            url: getDataUrl,
            pageSize: 30,
            preFill: false,
            filter: true,
            hidePagerOnEmpty: true,
            checkboxes: true,
            columnPicker: true,
            types: {
              string: {
                placeHolder: "Filter"
              },
              number: {
                decimals: 2
              },
              date: {
                utc: false,
                datePicker: false
              }
            },
            rowClicked: function(data) {
              if (data.checked) {
                that.trigger(that.attr.table_id, "tableCheckboxSelected", {name: data.row.name});
              }
              else {
                that.trigger(that.attr.table_id, "tableCheckboxDeselected", {name: data.row.name});
              }
              switch(that.attr.table_name) {
                case 'executions':
                  data.event.preventDefault();
                  that._showLog(data.row.uuid);
                  break;
                case 'checks':
                  data.event.preventDefault();
                  that._showFile(data.row.name);
                  break;
              }
            },
            tableCreated: function(data) {
              that._replaceScheduleWithHumanNames();
              that.trigger(that.attr.table_id, "tableReady");
            }
          }).data('WATable');
        } else {
          that.trigger(that.attr.table_id, "update");
        }
        $(this.attr.table_id).show();
      };

      this._replaceScheduleWithHumanNames = function() {
        // Move this to its own file
        $(".watable-col-schedule").each(function(idx, element) {
          $(element).jqCron({
            enabled_minute: true,
            multiple_dom: true,
            multiple_month: true,
            multiple_mins: true,
            multiple_dow: true,
            multiple_time_hours: true,
            multiple_time_minutes: true,
            default_value: $(element).text()
          });
          $(element).children("jqCronContainer").hide();
          var human_text = $(element).jqCronGetInstance().getHumanText();
          $(element).text(human_text);
        });
      };

      this._showLog = function (uuid) {
        $.ajax({
          type: "GET",
          url: '/admin/checker/api/executions/stream_structured/'+uuid,
          success: function(data) {
            $('#dialogModal .modal-body').text(data);
            $('#dialogModal').modal('show');
          }
        });
      };

      this._showFile = function(uuid) {
        $.ajax({
          type: "GET",
          url: '/admin/checker/api/checks/stream_check/'+uuid,
          success: function(data) {
            $('#dialogModal .modal-body').text(data);
            $('#dialogModal .modal-title').text('');
            $('#dialogModal').modal('show');
          }
        });
      };

      this.after('initialize', function() {
        // Listeners
        var table_id = "#" + this.node.id;
        this.on(table_id, "load", this.loadTable);
        this.on(table_id, "hide", this.hideTable);
        this.on(table_id, "update", this.updateTable);

        // Log
        console.log("js/checker/admin/table loaded");
      });
    }
  }
);
