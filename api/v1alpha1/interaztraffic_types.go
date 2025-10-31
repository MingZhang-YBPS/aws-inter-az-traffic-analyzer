/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// InterAZTrafficSpec defines the desired state of InterAZTraffic.
type InterAZTrafficSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	//+optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec.startFrom is immutable"
	StartFrom metav1.Time `json:"startFrom,omitempty"`
	//+optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec.endTo is immutable"
	EndTo *metav1.Time `json:"endTo,omitempty"`

	// VPCId is the vpc id where this EKS cluster exists
	// Currently, I don't find anyway to retrieve vpc ID from EKS pod, so have to pass it from outside
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec.vpcId is immutable"
	VPCId string `json:"vpcId"`
}

// InterAZTrafficStatus defines the observed state of InterAZTraffic.
type InterAZTrafficStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// LastestReportLocation is the latest report location in S3, like "s3://the-bucket-name/the-object-name"
	// +optional
	LastestReportLocation string `json:"lastestReportLocation,omitempty"`

	// +optional
	LastestReportCreationTimeStamp *metav1.Time `json:"lastestReportCreationTimeStamp,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// InterAZTraffic is the Schema for the interaztraffics API.
type InterAZTraffic struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   InterAZTrafficSpec   `json:"spec,omitempty"`
	Status InterAZTrafficStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// InterAZTrafficList contains a list of InterAZTraffic.
type InterAZTrafficList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []InterAZTraffic `json:"items"`
}

func init() {
	SchemeBuilder.Register(&InterAZTraffic{}, &InterAZTrafficList{})
}
