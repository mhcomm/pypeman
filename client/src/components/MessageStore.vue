<template>
  <v-layout row wrap>
    <v-flex sm12 class="channels">
      <h2>Channel list</h2>
      {{pagination}}

      <v-data-table
        :headers="headers"
        :items="messages"
        :pagination.sync="pagination"
        :loading="loading"
        :total-items="totalItems"
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
      <div class="text-xs-center pt-2">
        <v-pagination v-model="pagination.page" :length="pages"></v-pagination>
      </div>
    </v-flex>
    <v-snackbar :timeout="2000" :top="true" :multi-line="'multi-line'" v-model="success" :color="'success'">
      Message replayed with success.
    </v-snackbar>
  </v-layout>
</template>

<script>

export default {
  name: 'MessageStore',
  created () {
    this.channelName = this.$route.params.channelName
    this.loadMessages()
  },
  watch: {
    '$route' (to, from) {
      // react to route change
    },
    pagination: {
      handler () {
        this.loadMessages()
      },
      deep: true
    }
  },
  methods: {
    loadMessages () {
      this.loading = true
      let start = (this.pagination.page - 1) * this.pagination.rowsPerPage
      let args = [this.channelName, start, this.pagination.rowsPerPage]
      if (this.pagination.sortBy !== null) {
        args.push((this.pagination.descending ? '' : '-') + this.pagination.sortBy)
      }
      this.$clientcall('list_msg', args).then((result) => {
        this.messages = result.messages
        this.totalItems = result.total
        this.loading = false
      })
    },
    replayMessage (msg) {
      this.$clientcall('replay_msg', [this.channelName, [msg.id]]).then((result) => {
        this.success = true
        window.setTimeout(this.loadMessages, 1000) // TODO Quirck Hack to avoid jsonrpcclient bug
      })
    },
    toggleOrder () {
      this.pagination.descending = !this.pagination.descending
    }
  },
  computed: {
    pages () {
      return Math.ceil(this.totalItems / this.pagination.rowsPerPage)
    }
  },
  data () {
    return {
      channelName: null,
      messages: [],
      error: false,
      success: false,
      loading: true,
      totalItems: 0,
      headers: [
        {text: 'Timestamp', align: 'left', value: 'timestamp'},
        {text: 'Message ID', align: 'left', value: 'id'},
        {text: 'State', value: 'state'},
        {text: 'Action', value: 'action'}
      ],
      pagination: {
        sortBy: 'timestamp',
        descending: true,
        page: 1,
        rowsPerPage: 3
      }
    }
  }
}
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>

</style>
