// The Vue build version to load with the `import` command
// (runtime-only or standalone) has been set in webpack.base.conf with an alias.
import Vue from 'vue'
import vueConfig from 'vue-config'
import App from './App'
import router from './router'
import Vuetify from 'vuetify'
import PypemanClient from './plugin/pypemanClient'
import 'vuetify/dist/vuetify.min.css'
import './assets/styles/roboto.css'
import configs from 'configs'

Vue.config.productionTip = false

Vue.use(Vuetify)
Vue.use(PypemanClient, {url: configs.serverConfig})
Vue.use(vueConfig, configs)

/* eslint-disable no-new */
new Vue({
  el: '#app',
  router,
  template: '<App/>',
  components: { App }
})
