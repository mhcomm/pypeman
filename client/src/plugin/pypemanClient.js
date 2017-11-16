import Client from 'jsonrpc-websocket-client'
import eventHub from '../eventHub'

export default {
  install (Vue, options) {
    let url = options.url
    let client = new Client(url)

    Vue.prototype.$clientcall = function (path, args) {
      // console.log(path, args, client.status)
      if (client.status === 'closed') { // TODO handle 'connecting' status
        return client.open().then(() => {
          return client.call(path, args).catch((err) => {
            eventHub.$emit('clienterror', err)
          })
        })
      } else {
        return client.call(path, args).catch((err) => {
          eventHub.$emit('clienterror', err)
        })
      }
    }
  }

}
