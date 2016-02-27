var $ = require("jQuery");
var session = require('app/session');

const api = {

  post(path, data){
    return api.ajax({url: path, data: JSON.stringify(data), type: 'POST'}, false);
  },

  get(path){
    return api.ajax({url: path});
  },

  ajax(cfg, withToken = true){
    var defaultCfg = {
      type: "GET",
      dataType: "json",
      beforeSend: function(xhr) {
        if(withToken){
          var { token } = session.getUserData();
          xhr.setRequestHeader('Authorization','Bearer ' + token);
        }
       }
    }

    return $.ajax($.extend({}, defaultCfg, cfg));
  }
}

module.exports = api;