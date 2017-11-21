import Vue from 'vue'
import Channels from '@/components/Channels'

describe('Channels.vue', () => {
  it('should render correct contents', () => {
    const Constructor = Vue.extend(Channels)
    const vm = new Constructor().$mount()
    expect(vm.$el.querySelector('.channels h2').textContent)
      .to.equal('Channel list')
  })
})
