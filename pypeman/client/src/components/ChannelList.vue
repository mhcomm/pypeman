<template>
  <ul class="channel-list">
    <li v-for="channel in channels" :key="channel.name">
      <div class="row" @click.stop="handleRowClick(channel)">
        <div class="name">{{channel.name}}</div>

        <div class="store">
          <router-link v-if="channel.has_message_store"
            router-link :to="{ name: 'messagestore', params: {channelName: channel.name} }"
          >
            Show messages
          </router-link>
        </div>

        <div class="processed">
            {{ channel.processed }} message(s)
        </div>

        <div class="status">
            {{ channel.status }}
            (<a v-if="channel.status === 'WAITING'" color="red" @click.stop="changeState(channel, 'stop')">Stop</a>
            <a v-if="channel.status === 'STOPPED'" color="green" @click.stop="changeState(channel, 'start')">Start</a>)
        </div>

        <div class="showsubs">
          <i aria-hidden="true" class="material-icons icon" v-if="channel.subchannels.length && !channel.showSub">keyboard_arrow_down</i>
          <i aria-hidden="true" class="material-icons icon" v-if="channel.subchannels.length && channel.showSub">keyboard_arrow_up</i>
        </div>
      </div>

      <div v-if="channel.showSub">
        <channel-list v-if="channel.subchannels && channel.subchannels.length" :channels="channel.subchannels"></channel-list>
      </div>
    </li>
  </ul>
</template>

<script>
import Vue from 'vue'
import Channel from '@/components/ChannelList'

export default {
  name: 'channel-list',
  props: ['channels', 'folded'],
  created () {
  },
  components: {
    'channel-list': Channel
  },
  methods: {
    changeState (chan, state) {
      this.$clientcall(state + '_channel', [chan.name]).then((result) => {
        chan.status = result.status
      }, this.showError)
    },
    handleRowClick (channel) {
      Vue.set(channel, 'showSub', !channel.showSub)
    },
    showError (err) {
      console.log('Error while calling server', err)
    }
  },
  data () {
    return {
      headers: [
        {text: 'Channel name', align: 'left', value: 'name'},
        {text: 'Msg Processed', value: 'processed'},
        {text: 'Status', value: 'status'},
        {text: 'Subchannels', value: 'subchannel'},
        {text: 'Msg store', value: 'msgstore'}
      ],
      pagination: {
        sortBy: 'name'
      }
    }
  }
}
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
a{
  cursor: pointer;
}

.channel-list{
  list-style-type: none;
}

.row{
  display: block;
  width: 100%;
  display: flex;
  border-bottom: 1px solid hsla(0,0%,100%,.12);
  text-align: right;
  font-size: 1.2em;
  height: 3em;
  line-height: 3em;
  cursor: pointer;
}

.channel-list .row:first-child{
  /*border-top: 1px solid hsla(0,0%,100%,.12);*/
}

.row > div{
  /*flex: 1;*/
  padding-right: 10px;
}

.showsubs{
  width: 2em;
}
.row .name{
  flex: 1;
  text-align: left;
}

.row .status{
}

.channel-list .channel-list{
  padding-left: 1em;
  border-left: 2px solid hsla(0,0%,100%,.12);
  margin-top: 5px;
  margin-left: 1em;
  /*font-size: 0.9em;*/
}
</style>
