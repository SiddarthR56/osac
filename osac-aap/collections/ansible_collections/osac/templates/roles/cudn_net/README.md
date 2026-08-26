# cudn_net

Provisions networking resources using ClusterUserDefinedNetwork (CUDN) on OpenShift.

> **Note:** Some resource families are implemented by reusable K8s-native roles rather than
> directly in `cudn_net`.
>
> - SecurityGroup enforcement (NetworkPolicy) lives in the standalone
>   `osac.templates.network_policy` role.
> - ExternalIPPool / ExternalIP / ExternalIPAttachment lifecycle lives in
>   `osac.templates.metallb_l2`.
>
> The dispatcher still resolves those resource kinds to this NetworkClass's fabric manager
> (`cudn_net`), so `cudn_net` must expose matching task entrypoints and delegate internally
> (see [Task Files](#task-files)).

## Resources

### VirtualNetwork

VirtualNetworks define the top-level network isolation boundary with CIDR allocation and implementation strategy selection via NetworkClass.

**Key behaviors:**
- Creates ClusterUserDefinedNetwork CR in the cluster
- Supports IPv4-only, IPv6-only, and dual-stack configurations
- NetworkClass determines the implementation strategy (cudn_net)
- One VirtualNetwork maps to one ClusterUserDefinedNetwork

**Implementation:**
- ClusterUserDefinedNetwork is a cluster-scoped resource
- CIDR ranges are configured via spec.network field
- Layer2 topology is used for CUDN implementation

### Subnet

Subnets subdivide VirtualNetworks into logical segments with isolated namespaces for workload deployment.

**Key behaviors:**
- Creates namespace with specific labels for CUDN attachment
- Namespace labeled with `osac.openshift.io/virtual-network: {vn-name}`
- Namespace labeled with `k8s.ovn.org/primary-user-defined-network: ""`
- Enables pod connectivity to the parent VirtualNetwork's CUDN
- One Subnet maps to one namespace

**Namespace targeting:**
- Pods deployed in the Subnet namespace automatically connect to the VirtualNetwork's CUDN
- No additional network configuration needed on pods
- Network attachment is namespace-based, not interface-based

## Implementation Strategy

This role implements the `cudn_net` NetworkClass strategy using OpenShift's ClusterUserDefinedNetwork (CUDN) feature. The implementation follows these patterns:

**For VirtualNetworks:**
- Create ClusterUserDefinedNetwork CR with Layer2 topology
- Configure CIDR ranges from VirtualNetwork spec

**For Subnets:**
- Create namespace with CUDN attachment labels
- Label namespace with parent VirtualNetwork reference
- Pods deployed in namespace automatically connect to CUDN

## Task Files

- `tasks/create_virtual_network.yaml` - Creates ClusterUserDefinedNetwork CR from VirtualNetwork resource
- `tasks/delete_virtual_network.yaml` - Removes ClusterUserDefinedNetwork CR
- `tasks/create_subnet.yaml` - Creates namespace with CUDN labels from Subnet resource
- `tasks/delete_subnet.yaml` - Removes namespace
- `tasks/create_security_group.yaml` - Delegates to `osac.templates.network_policy` (`create_security_group`)
- `tasks/delete_security_group.yaml` - Delegates to `osac.templates.network_policy` (`delete_security_group`)
- `tasks/create_external_ip_pool.yaml` - Delegates to `osac.templates.metallb_l2` (`create_external_ip_pool`)
- `tasks/delete_external_ip_pool.yaml` - Delegates to `osac.templates.metallb_l2` (`delete_external_ip_pool`)
- `tasks/create_external_ip.yaml` - Delegates to `osac.templates.metallb_l2` (`create_external_ip`)
- `tasks/delete_external_ip.yaml` - Delegates to `osac.templates.metallb_l2` (`delete_external_ip`)
- `tasks/attach_external_ip.yaml` - Delegates to `osac.templates.metallb_l2` (`attach_external_ip`)
- `tasks/detach_external_ip.yaml` - Delegates to `osac.templates.metallb_l2` (`detach_external_ip`)

## Usage

### Example: VirtualNetwork Provisioning

```yaml
- name: Create VirtualNetwork
  ansible.builtin.include_role:
    name: cudn_net
    tasks_from: create_virtual_network
  vars:
    virtual_network: "{{ osac_job_vars.resource }}"
    virtual_network_name: "{{ osac_job_vars.resource.metadata.name }}"
```

### Example: Subnet Provisioning

```yaml
- name: Create Subnet
  ansible.builtin.include_role:
    name: cudn_net
    tasks_from: create_subnet
  vars:
    subnet: "{{ osac_job_vars.resource }}"
    subnet_name: "{{ osac_job_vars.resource.metadata.name }}"
```
