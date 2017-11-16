import Client from 'jsonrpc-websocket-client'
import eventHub from '../eventHub'

export default {
  install (Vue, options) {
    let url = options.url
    let client = new Client(url)

    Vue.prototype.$clientcall = function (path, args) {
      let prm = new Promise((resolve, reject) => {
        if (client.status === 'closed') { // TODO handle 'connecting' status
          client.open().then(() => {
            client.call(path, args).then((result) => {
              resolve(result)
            }, (err) => {
              console.log('Error while doing request but keep retrying', err)
              client.call(path, args).then((result) => {
                resolve(result)
              }, (err) => {
                eventHub.$emit('clienterror', err)
              })
            })
          })
        } else {
          client.call(path, args).then((result) => {
            resolve(result)
          }, (err) => {
            console.log('Error while doing request but keep retrying', err)
            client.open().then(() => {
              client.call(path, args).then((result) => {
                resolve(result)
              }, (err) => {
                eventHub.$emit('clienterror', err)
              })
            })
          })
        }
      })
      return prm
    }
  }

}
