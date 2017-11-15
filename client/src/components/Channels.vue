<template>
  <v-layout row wrap>
    <v-flex sm12 class="channels">
      <h2>Channel list</h2>
      <channel-list :channels="channels"></channel-list>
    </v-flex>
    <v-snackbar :timeout="2000" :top="true" :multi-line="'multi-line'" v-model="snackbar" :color="'error'">
      An error while doing server query.
      <v-btn flat color="pink" @click.native="snackbar = false">Close</v-btn>
    </v-snackbar>
  </v-layout>
</template>

<script>
import Client from 'jsonrpc-websocket-client'
import ChannelList from '@/components/ChannelList'

export default {
  name: 'Channels',
  created () {
    this.client = new Client('ws://localhost:8765')
    this.client.open().then(() => {
      this.loadChannels()
    }, this.showError)
  },
  components: {
    'channel-list': ChannelList
  },
  methods: {
    loadChannels () {
      this.client.call('channels', []).then((result) => {
        console.log(result)
        this.channels = result
      }, this.showError)
    },
    selectChannel (chan) {
      console.log('Cliked', chan.name)
    },
    changeState (chan, state) {
      this.client.open().then(() => {
        this.client.call(state + '_channel', [chan.name]).then((result) => {
          chan.status = result.status
        }, this.showError)
      }, this.showError)
    },
    showError (err) {
      this.snackbar = true
      console.log('Error while calling server', err)
    }
  },
  data () {
    return {
      channels: [],
      client: null,
      snackbar: false
    }
  }
}
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
</style>
