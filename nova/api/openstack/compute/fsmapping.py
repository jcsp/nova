# Copyright 2016 Red Hat Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""Extension for Filesystem Mapping objects"""

from oslo_utils import strutils
from webob import exc

from nova.api.openstack import api_version_request
from nova.api.openstack import common
from nova.api.openstack.compute.schemas import volumes as volumes_schema
from nova.api.openstack import extensions
from nova.api.openstack import wsgi
from nova.api import validation
from nova import compute
from nova.compute import vm_states
from nova import exception
from nova.i18n import _
from nova import objects
from nova import volume

ALIAS = "os-fsmappings"
authorize = extensions.os_compute_authorizer(ALIAS)
authorize_attach = extensions.os_compute_authorizer('os-fsmappings-attachments')


create_fs_mapping = {
    'type': 'object',
    'properties': {
        'filesystemMapping': {
            'type': 'object',
            'properties': {
                'volumeId': parameter_types.volume_id,
                'device': {
                    'type': ['string', 'null'],
                    # NOTE: The validation pattern from match_device() in
                    #       nova/block_device.py.
                    'pattern': '(^/dev/x{0,1}[a-z]{0,1}d{0,1})([a-z]+)[0-9]*$'
                }
            },
            'required': ['volumeId'],
            'additionalProperties': False,
        },
    },
    'required': ['filesystemMapping'],
    'additionalProperties': False,
}


ganesha_template = """
NFS_Core_param
{
Plugins_Dir = "/home/john/nfs-ganesha/src/build/FSAL/FSAL_VFS/vfs";
}

EXPORT {
        Path = "/scratch/nfs";
        Pseudo = "/";
        Export_Id = 1;
        Protocols = "4,nfsvsock";
        Access_Type = RW;
        Squash = No_root_squash;
        SecType = "sys";
        FSAL
        {
                Name = VFS;
        }
}
"""

# TODO: make configurable, this is the hypervisor-side config path
GANESHA_CONFIG_PATH = "/etc/ganesha/ganesha.conf"

from manilaclient import client as manila_client
from keystoneauth1 import loading as ks_loading

_SESSION = None

def get_manila_client(context):
    global _SESSION

    if not _SESSION:
        _SESSION = ks_loading.load_session_from_conf_options(CONF,
                                                             CINDER_OPT_GROUP)

    auth = context.get_auth_plugin()

    # Version selected to be just new enough to have the Manila/Ceph features
    manila_client.Client("2.13",
                         session=_SESSION,
                         auth=auth)


class FilesystemMappingController(wsgi.Controller):


    # Remoting... so, in the libvirt case we have a domainXML thinger
    # that goes via a libvirt connection and writes the XML for accessing
    # an RBD image.  Nova per se doesn't have to connect to the hypervisor.

    def _enable_vsock(self, instance_id):
        # We need handle to the libvirt connection
        #self.compute_api = compute.API(skip_policy_check=True)
        pass

    def _authorize_manila(self, context, mapping_id, share_id):
        # We need a context
        mc = get_manila_client(context)

        manila_auth_name = mapping_id

        mc.authorize(share_id, manila_auth_name)
        pass


    def _update_ganesha(self):
        # HACK for demo; sidestep the issue of how we remote to the hypervisor
        # host by just writing the ganesha config locally
        open(GANESHA_CONFIG_PATH, "w").write(ganesha_template)
        pass

    @extensions.expected_errors(404)
    def show(self, req, server_id, id):
        # """Return data about the given volume attachment."""
        # context = req.environ['nova.context']
        # authorize(context)
        # authorize_attach(context, action='show')
        #
        # volume_id = id
        # instance = common.get_instance(self.compute_api, context, server_id)
        #
        # bdms = objects.BlockDeviceMappingList.get_by_instance_uuid(
        #         context, instance.uuid)
        #
        # if not bdms:
        #     msg = _("Instance %s is not attached.") % server_id
        #     raise exc.HTTPNotFound(explanation=msg)
        #
        # assigned_mountpoint = None
        #
        # for bdm in bdms:
        #     if bdm.volume_id == volume_id:
        #         assigned_mountpoint = bdm.device_name
        #         break
        #
        # if assigned_mountpoint is None:
        #     msg = _("volume_id not found: %s") % volume_id
        #     raise exc.HTTPNotFound(explanation=msg)
        #
        # return {'volumeAttachment': _translate_attachment_detail_view(
        #     volume_id,
        #     instance.uuid,
        #     assigned_mountpoint)}
        return {'filesystemMapping': {}}

    @extensions.expected_errors((400, 404, 409))
    @validation.schema(volumes_schema.create_volume_attachment)
    def create(self, req, server_id, body):
        """Attach a volume to an instance."""
        context = req.environ['nova.context']
        authorize(context)
        authorize_attach(context, action='create')

        # volume_id = body['volumeAttachment']['volumeId']
        # device = body['volumeAttachment'].get('device')
        #
        # instance = common.get_instance(self.compute_api, context, server_id)
        #
        # if instance.vm_state in (vm_states.SHELVED,
        #                          vm_states.SHELVED_OFFLOADED):
        #     _check_request_version(req, '2.20', 'attach_volume',
        #                            server_id, instance.vm_state)
        #
        # try:
        #     device = self.compute_api.attach_volume(context, instance,
        #                                             volume_id, device)
        # except exception.InstanceUnknownCell as e:
        #     raise exc.HTTPNotFound(explanation=e.format_message())
        # except exception.VolumeNotFound as e:
        #     raise exc.HTTPNotFound(explanation=e.format_message())
        # except exception.InstanceIsLocked as e:
        #     raise exc.HTTPConflict(explanation=e.format_message())
        # except exception.InstanceInvalidState as state_error:
        #     common.raise_http_conflict_for_instance_invalid_state(state_error,
        #             'attach_volume', server_id)
        # except (exception.InvalidVolume,
        #         exception.InvalidDevicePath) as e:
        #     raise exc.HTTPBadRequest(explanation=e.format_message())
        #
        # # The attach is async
        # attachment = {}
        # attachment['id'] = volume_id
        # attachment['serverId'] = server_id
        # attachment['volumeId'] = volume_id
        # attachment['device'] = device
        #
        # # NOTE(justinsb): And now, we have a problem...
        # # The attach is async, so there's a window in which we don't see
        # # the attachment (until the attachment completes).  We could also
        # # get problems with concurrent requests.  I think we need an
        # # attachment state, and to write to the DB here, but that's a bigger
        # # change.
        # # For now, we'll probably have to rely on libraries being smart
        #
        # # TODO(justinsb): How do I return "accepted" here?
        # return {'volumeAttachment': attachment}
        return {'filesystemMapping': {}}


class FilesystemMappings(extensions.V21APIExtensionBase):
    name = "FilesystemMappings"
    alias = ALIAS
    version = 1

    def get_resources(self):
        resources = []

        res = extensions.ResourceExtension(
            ALIAS, FilesystemMappingController(), collection_actions={'detail': 'GET'})
        resources.append(res)
        res = extensions.ResourceExtension('os-filesystem_mappings',
                                           FilesystemMappingController(),
                                           parent=dict(
                                                member_name='server',
                                                collection_name='servers'))
        resources.append(res)

        return resources

    def get_controller_extensions(self):
        return []


class VolumeAttachmentController(wsgi.Controller):
    """The volume attachment API controller for the OpenStack API.

    A child resource of the server.  Note that we use the volume id
    as the ID of the attachment (though this is not guaranteed externally)

    """

    def __init__(self):
        self.compute_api = compute.API(skip_policy_check=True)
        self.volume_api = volume.API()
        super(VolumeAttachmentController, self).__init__()

    @extensions.expected_errors(404)
    def index(self, req, server_id):
        """Returns the list of volume attachments for a given instance."""
        context = req.environ['nova.context']
        authorize_attach(context, action='index')
        return self._items(req, server_id,
                           entity_maker=_translate_attachment_summary_view)

    @wsgi.response(202)
    @extensions.expected_errors((400, 404, 409))
    @validation.schema(volumes_schema.update_volume_attachment)
    def update(self, req, server_id, id, body):
        context = req.environ['nova.context']
        authorize(context)
        authorize_attach(context, action='update')

        old_volume_id = id
        try:
            old_volume = self.volume_api.get(context, old_volume_id)

            new_volume_id = body['volumeAttachment']['volumeId']
            new_volume = self.volume_api.get(context, new_volume_id)
        except exception.VolumeNotFound as e:
            raise exc.HTTPNotFound(explanation=e.format_message())

        instance = common.get_instance(self.compute_api, context, server_id)

        bdms = objects.BlockDeviceMappingList.get_by_instance_uuid(
                context, instance.uuid)
        found = False
        try:
            for bdm in bdms:
                if bdm.volume_id != old_volume_id:
                    continue
                try:
                    self.compute_api.swap_volume(context, instance, old_volume,
                                                 new_volume)
                    found = True
                    break
                except exception.VolumeUnattached:
                    # The volume is not attached.  Treat it as NotFound
                    # by falling through.
                    pass
                except exception.InvalidVolume as e:
                    raise exc.HTTPBadRequest(explanation=e.format_message())
        except exception.InstanceIsLocked as e:
            raise exc.HTTPConflict(explanation=e.format_message())
        except exception.InstanceInvalidState as state_error:
            common.raise_http_conflict_for_instance_invalid_state(state_error,
                    'swap_volume', server_id)

        if not found:
            msg = _("The volume was either invalid or not attached to the "
                    "instance.")
            raise exc.HTTPNotFound(explanation=msg)

    @wsgi.response(202)
    @extensions.expected_errors((400, 403, 404, 409))
    def delete(self, req, server_id, id):
        """Detach a volume from an instance."""
        context = req.environ['nova.context']
        authorize(context)
        authorize_attach(context, action='delete')

        volume_id = id

        instance = common.get_instance(self.compute_api, context, server_id)
        if instance.vm_state in (vm_states.SHELVED,
                                 vm_states.SHELVED_OFFLOADED):
            _check_request_version(req, '2.20', 'detach_volume',
                                   server_id, instance.vm_state)
        try:
            volume = self.volume_api.get(context, volume_id)
        except exception.VolumeNotFound as e:
            raise exc.HTTPNotFound(explanation=e.format_message())

        bdms = objects.BlockDeviceMappingList.get_by_instance_uuid(
                context, instance.uuid)
        if not bdms:
            msg = _("Instance %s is not attached.") % server_id
            raise exc.HTTPNotFound(explanation=msg)

        found = False
        try:
            for bdm in bdms:
                if bdm.volume_id != volume_id:
                    continue
                if bdm.is_root:
                    msg = _("Can't detach root device volume")
                    raise exc.HTTPForbidden(explanation=msg)
                try:
                    self.compute_api.detach_volume(context, instance, volume)
                    found = True
                    break
                except exception.VolumeUnattached:
                    # The volume is not attached.  Treat it as NotFound
                    # by falling through.
                    pass
                except exception.InvalidVolume as e:
                    raise exc.HTTPBadRequest(explanation=e.format_message())
                except exception.InstanceUnknownCell as e:
                    raise exc.HTTPNotFound(explanation=e.format_message())
                except exception.InvalidInput as e:
                    raise exc.HTTPBadRequest(explanation=e.format_message())

        except exception.InstanceIsLocked as e:
            raise exc.HTTPConflict(explanation=e.format_message())
        except exception.InstanceInvalidState as state_error:
            common.raise_http_conflict_for_instance_invalid_state(state_error,
                    'detach_volume', server_id)

        if not found:
            msg = _("volume_id not found: %s") % volume_id
            raise exc.HTTPNotFound(explanation=msg)

    def _items(self, req, server_id, entity_maker):
        """Returns a list of attachments, transformed through entity_maker."""
        context = req.environ['nova.context']
        authorize(context)

        instance = common.get_instance(self.compute_api, context, server_id)

        bdms = objects.BlockDeviceMappingList.get_by_instance_uuid(
                context, instance.uuid)
        limited_list = common.limited(bdms, req)
        results = []

        for bdm in limited_list:
            if bdm.volume_id:
                results.append(entity_maker(bdm.volume_id,
                                            bdm.instance_uuid,
                                            bdm.device_name))

        return {'volumeAttachments': results}



def _translate_snapshot_summary_view(context, vol):
    """Maps keys for snapshots summary view."""
    d = {}

    d['id'] = vol['id']
    d['volumeId'] = vol['volume_id']
    d['status'] = vol['status']
    # NOTE(gagupta): We map volume_size as the snapshot size
    d['size'] = vol['volume_size']
    d['createdAt'] = vol['created_at']
    d['displayName'] = vol['display_name']
    d['displayDescription'] = vol['display_description']
    return d



class Volumes(extensions.V21APIExtensionBase):
    """Volumes support."""

    name = "Volumes"
    alias = ALIAS
    version = 1

    def get_resources(self):
        resources = []

        res = extensions.ResourceExtension(
            ALIAS, VolumeController(), collection_actions={'detail': 'GET'})
        resources.append(res)

        res = extensions.ResourceExtension('os-volumes_boot',
                                           inherits='servers')
        resources.append(res)

        res = extensions.ResourceExtension('os-volume_attachments',
                                           VolumeAttachmentController(),
                                           parent=dict(
                                                member_name='server',
                                                collection_name='servers'))
        resources.append(res)

        res = extensions.ResourceExtension(
            'os-snapshots', SnapshotController(),
            collection_actions={'detail': 'GET'})
        resources.append(res)

        return resources

    def get_controller_extensions(self):
        return []
