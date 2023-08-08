from aiohttp import web

from pypeman.plugins.remoteadmin import views


def init_urls(app, prefix=""):
    """
    Create the pypeman remoteadmin routing

    Args:
        app (aiohttp.web.Application): The aiohttp web app where the
                                        url routings have to be added
    """
    app.add_routes([
        # API :
        web.get(prefix + '/channels', views.list_channels),
        web.get(prefix + '/channels/{channelname}/start', views.start_channel),
        web.get(prefix + '/channels/{channelname}/stop', views.stop_channel),
        web.get(prefix + '/channels/{channelname}/messages', views.list_msgs),
        web.get(prefix + '/channels/{channelname}/messages/{message_id}/replay', views.replay_msg),
        web.get(prefix + '/channels/{channelname}/messages/{message_id}/view', views.view_msg),
        web.get(prefix + '/channels/{channelname}/messages/{message_id}/preview', views.preview_msg),
        # WEBSOCKETS :
        web.get(prefix + '/', views.backport_old_client),
    ])
