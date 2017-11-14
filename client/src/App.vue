<template>
  <v-app id="pypeman" dark>
    <v-navigation-drawer
      app
      fixed
      temporary
      v-model="drawer"
      clipped
      >
      <v-toolbar flat>
        <v-list>
          <v-list-tile>
            <v-list-tile-title class="title">
              Menu
            </v-list-tile-title>
          </v-list-tile>
        </v-list>
      </v-toolbar>
      <v-divider></v-divider>
      <v-list dense>
        <v-list-tile v-for="item in items" :key="item.title" router-link :to="item.route">
          <v-list-tile-action v-if="item.icon">
            <v-icon>{{ item.icon }}</v-icon>
          </v-list-tile-action>
          <v-list-tile-content>
            <v-list-tile-title>{{ item.title }}</v-list-tile-title>
          </v-list-tile-content>
        </v-list-tile>
      </v-list>
    </v-navigation-drawer>
    <v-toolbar app fixed>
      <v-toolbar-side-icon @click.stop="drawer = !drawer"></v-toolbar-side-icon>
      <v-toolbar-title>Pypeman admin</v-toolbar-title>
    </v-toolbar>
    <main>
      <v-content>
        <v-container fluid>
              <router-view></router-view>
        </v-container>
      </v-content>
    </main>
    <v-footer app></v-footer>
  </v-app>
</template>

<script>
export default {
  name: 'app',
  data () {
    return {
      drawer: false,
      items: [
        {title: 'Channels', icon: null, route: {name: 'channels'}},
        {title: 'Other', icon: null, route: {name: 'home'}}
      ]
    }
  }
}
</script>

<style>
#app {
  font-family: "Avenir", Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
}
</style>
