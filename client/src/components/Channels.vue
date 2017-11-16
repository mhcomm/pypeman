<template>
  <v-layout row wrap>
    <v-flex sm12 class="channels">
      <h2>Channel list</h2>
      <channel-list :channels="channels"></channel-list>
    </v-flex>
  </v-layout>
</template>

<script>
// import Client from 'jsonrpc-websocket-client'
import ChannelList from '@/components/ChannelList'

export default {
  name: 'Channels',
  created () {
    this.loadChannels()
  },
  components: {
    'channel-list': ChannelList
  },
  methods: {
    loadChannels () {
      this.$clientcall('channels', []).then((result) => {
        this.channels = result
      }, this.showError)
    }
  },
  data () {
    return {
      channels: []
    }
  }
}
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
</style>
