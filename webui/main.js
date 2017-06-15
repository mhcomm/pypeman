
uri_valid = 'ws://localhost:8080/ws';

// Set up WAMP connection to router
var connection = new autobahn.Connection({
   url: uri_valid,
   realm: 'realm1'}
);

var wampConnection = new Promise(function(resolve, reject) {
  connection.onopen = function (session) {
    resolve(session);
    onConnect.resolve(session);
  }
});

connection.open();

var viewData = {
  channels: {},
}

function update_channel_view(session, view_data)
{
  session.call('pypeman.list_channels').then(
     function (channels) {
       viewData.channels = channels;
     },
     function (error) {
        console.log("Call to update_channel_view failed:", error);
     }
  );
}

function process_state_change(session, channel_data)
{
    data = channel_data[0];
    name = data["channel"];
    old_state = data["old_state"];
    new_state = data["state"];
    console.log("event state change: channel'", name, "' state: '", old_state, "' -> '", new_state, "'");
    update_channel_view(session, viewData);
}

wampConnection.then(function(session) {
  console.log("Wamp Connection successfull");
  update_channel_view(session, viewData);

  session.subscribe("pypeman.state_change", function(data){
    process_state_change(session, data);
  });

});

function toggleChannelState(session, channel)
{
  if (channel.state == "WAITING") {
    console.log("Stopping channel", channel.name);
    session.call('pypeman.stop_channel', [channel.name]).then(
      function (channel) {
          console.log(channel, "was successfully stopped");
      },
      function (error) {
          console.log("Stop channel failed :,", error);
      }
    );
  } 
  else if (channel.state == "STOPPED"){
    console.log("Starting channel", channel.name);
    session.call('pypeman.change_channel_state', [channel.name, "start"]).then(
      function (channel) {
          console.log(channel, "was successfully started");
      },
      function (error) {
          console.log("Start channel failed :,", error);
      }
    );
  }
}

var vm = new Vue({
  el: '#vue-instance',
  methods: {
    toggleState: function (channel) {
      wampConnection.then(function(session){
        toggleChannelState(session, channel)
      });
    }
  },
  computed: {
    
  },
  data: viewData,
});


