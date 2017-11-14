import Vue from 'vue'
import Router from 'vue-router'
import Channel from '@/components/Channel'
import MessageStore from '@/components/MessageStore'

Vue.use(Router)

export default new Router({
  routes: [
    {
      path: '/',
      redirect: '/channels'
    },
    {
      path: '/channels',
      name: 'channels',
      component: Channel
    },
    {
      path: '/channel/:channelName/message-store',
      name: 'messagestore',
      component: MessageStore
    }
  ]
})
