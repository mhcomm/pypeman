from aiohttp import web

from pypeman.plugins.remoteadmin import views


def init_urls(app):
    """
    Create the pypeman remoteadmin  routing

    Args:
        app (aiohttp.web.Application): The aiohttp web app where the
                                        url routing have to be added
    """
    app.add_routes([
        web.get('/channels', views.list_channels),
        web.get(r'/channels/{channelname}/start', views.list_channels),
        web.get(r'/channels/{channelname}/stop', views.stop_channel),
        web.get(r'/channels/{channelname}/messages', views.list_msgs),
        web.get(r'/channels/{channelname}/messages/{message_id}/replay', views.replay_msg),
        web.get(r'/channels/{channelname}/messages/{message_id}/view', views.view_msg),
        web.get(r'/channels/{channelname}/messages/{message_id}/preview', views.preview_msg),
    ])
