<template>
  <v-layout row wrap>
    <v-flex sm12 class="channels">
      <h2>Channel list</h2>

      <v-data-table
        :headers="headers"
        :items="messages"
        hide-actions
        class="elevation-1"
        item-key="id"
      >
        <template sloc="headers" slot-scope="props">
          <tr>
            <th v-for="header in props.headers" :key="header.text"
              :class="['column sortable', pagination.descending ? 'desc' : 'asc', header.value === pagination.sortBy ? 'active' : '']"
              @click="changeSort(header.value)"
            >
              <v-icon>arrow_upward</v-icon>
              {{ header.text }}
            </th>
          </tr>
        </template>
        <template slot="items" slot-scope="props">
          <tr @click="props.expanded = !props.expanded">
            <td>{{ props.item.timestamp }}</td>
            <td>{{ props.item.id }}</td>
            <td class="text-xs-right">{{ props.item.state }}</td>
            <td class="text-xs-right">
              <v-btn color="blue" @click.stop="replayMessage(props.item)">Replay</v-btn>
            </td>
          </tr>
        </template>
        <template slot="expand" slot-scope="props">
          <v-card flat>
            <v-card-text>{{ props.item.message }}</v-card-text>
          </v-card>
        </template>
      </v-data-table>
    </v-flex>
    <v-snackbar :timeout="2000" :top="true" :multi-line="'multi-line'" v-model="snackbar" :color="'error'">
      An error while doing server query.
      <v-btn flat color="pink" @click.native="snackbar = false">Close</v-btn>
    </v-snackbar>
  </v-layout>
</template>

<script>
import Client from 'jsonrpc-websocket-client'

export default {
  name: 'MessageStore',
  created () {
    this.channelName = this.$route.params.channelName
    this.client = new Client('ws://localhost:8765')
    this.loadMessages()
  },
  watch: {
    '$route' (to, from) {
      // react to route change
    }
  },
  methods: {
    loadMessages () {
      this.client.open().then(() => {
        this.client.call('list_msg', [this.channelName]).then((result) => {
          this.messages = result
        }, this.showError)
      }, this.showError)
    },
    selectChannel (chan) {
      console.log('Cliked', chan.name)
    },
    replayMessage (msg) {
      this.client.open().then(() => {
        this.client.call('replay_msg', [this.channelName, [msg.id]]).then((result) => {
          console.log('Success')
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
      channelName: null,
      messages: [],
      client: null,
      snackbar: false,
      headers: [
        {text: 'Timestamp', align: 'left', value: 'timestamp'},
        {text: 'Message ID', align: 'left', value: 'id'},
        {text: 'Status', value: 'status'},
        {text: 'Action', value: 'action'}
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

</style>
