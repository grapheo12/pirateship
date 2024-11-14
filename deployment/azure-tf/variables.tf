# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

variable "resource_group_location" {
  type        = string
  default     = "eastus"
  description = "Location of the resource group."
}

variable "platform_locations" {
  type        = list(string)
  # default     = ["northeurope", "westeurope", "eastus", "centralus", "westus"]
  default     = ["centralus"]
  description = "Location of the platforms/client machines."
}

variable "resource_group_name_prefix" {
  type        = string
  default     = "pft"
  description = "Prefix of the resource group name that's combined with a random ID so name is unique in your Azure subscription."
}

variable "username" {
  type        = string
  description = "The username for the local account that will be created on the new VM."
  default     = "pftadmin"
}

variable "sevpool_count" {
  type        = list(number)
  description = "Number of VMs with SEV"
  # default = [ 2, 0, 3, 0, 0 ]
  default     = [0]
}


variable "tdxpool_count" {
  type        = list(number)
  description = "Number of VMs with TDX"
  # default = [ 0, 0, 0, 3, 0 ]
  default     = [0]
}

variable "nonteepool_count" {
  type        = list(number)
  description = "Number of VMs without TEEs, used as replicas"
  # default = [ 0, 0, 0, 0, 0 ]
  default     = [4]
}

variable "clientpool_count"{
  type        = list(number)
  description = "Number of VMs with no trusted hardware, used as client nodes"
  # default = [ 0, 0, 0, 0, 3 ]
  default     = [3]
}

locals {
  platform_location_crossproduct_ = [ #(location_id, location_id) for peering
    for x in setproduct(
      range(length(var.platform_locations)),
      range(length(var.platform_locations))
    ): x if x[0] != x[1]
  ]

  sevpool_ids_flattened_ = tolist(setunion([ #(location_id, vm_id)
    for i, j in var.sevpool_count:[
      for k in range(j):
        [i, k]
    ]
  ]...))

  tdxpool_ids_flattened_ = tolist(setunion([
    for i, j in var.tdxpool_count:[
      for k in range(j):
        [i, k]
    ]
  ]...))

  nonteepool_ids_flattened_ = tolist(setunion([
    for i, j in var.nonteepool_count:[
      for k in range(j):
        [i, k]
    ]
  ]...))

  clientpool_ids_flattened_ = tolist(setunion([
    for i, j in var.clientpool_count:[
      for k in range(j):
        [i, k]
    ]
  ]...))

}