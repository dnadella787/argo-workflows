package oraclecloud

import (
	"context"
	"fmt"
	"github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	ocicommons "github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/common/auth"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	log "github.com/sirupsen/logrus"
	"io"
	"k8s.io/utils/pointer"
	"net/http"
	"os"
	"path"
	"path/filepath"
)

// ArtifactDriver is a driver for OCI Object Storage
type ArtifactDriver struct {
	authMode   v1alpha1.OracleAuthMode
	bucketName string
	region     string
}

// newOracleCloudClient returns an Oracle Cloud Object Storage Client
func (ad *ArtifactDriver) newOracleCloudClient() (*objectstorage.ObjectStorageClient, error) {
	ap, err := ad.newAuthProvider()
	if err != nil {
		return nil, err
	}

	c, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(ap)
	if err != nil {
		return nil, err
	}

	c.SetRegion(ad.region)
	return &c, nil
}

func (ad *ArtifactDriver) newAuthProvider() (ocicommons.ConfigurationProvider, error) {
	switch ad.authMode {
	case v1alpha1.WorkloadPrincipals:
		return auth.OkeWorkloadIdentityConfigurationProvider()
	case v1alpha1.InstancePrincipals:
		return auth.InstancePrincipalConfigurationProvider()
	default:
		return nil, fmt.Errorf("invalid AuthMode: %s for Oracle Cloud Object Storage", ad.authMode)
	}
}

func (ad *ArtifactDriver) Load(inputArtifact *v1alpha1.Artifact, localPath string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := ad.newOracleCloudClient()
	if err != nil {
		return err
	}

	ns, err := client.GetNamespace(ctx, objectstorage.GetNamespaceRequest{})
	if err != nil {
		return err
	}

	objPath := path.Join(inputArtifact.OracleCloud.Key, inputArtifact.Name)
	// Directly make GetObject call assuming that the artifact is a file
	object, err := client.GetObject(ctx, objectstorage.GetObjectRequest{
		NamespaceName: ns.Value,
		BucketName:    pointer.String(ad.bucketName),
		ObjectName:    pointer.String(objPath),
	})
	if err == nil {
		// copy the file to local storage
		return downloadFile(localPath, inputArtifact.Name, object.Content)
	}
	// the GetObject returned 404 so list by prefix in case the artifact
	// is a directory and download the entire directory
	if e, ok := err.(ocicommons.ServiceError); ok && e.GetHTTPStatusCode() == http.StatusNotFound {
		var nextStartsWith *string
		for {
			objs, err := client.ListObjects(ctx, objectstorage.ListObjectsRequest{
				NamespaceName: ns.Value,
				BucketName:    pointer.String(ad.bucketName),
				Prefix:        pointer.String(objPath),
				StartAfter:    nextStartsWith,
			})
			if err != nil {
				return err
			}
			nextStartsWith = objs.NextStartWith
			for _, obj := range objs.Objects {
				o, err := client.GetObject(ctx, objectstorage.GetObjectRequest{
					NamespaceName: ns.Value,
					BucketName:    pointer.String(ad.bucketName),
					ObjectName:    obj.Name,
				})
				if err != nil {
					return err
				}

				err = downloadFile(localPath, *obj.Name, o.Content)
				if err != nil {
					return err
				}
			}

			// paginated through all files
			if nextStartsWith == nil {
				break
			}
		}
		return nil
	}
	return err
}

func downloadFile(basePath string, fileName string, content io.ReadCloser) error {
	fullPath := path.Join(basePath, fileName)
	err := os.MkdirAll(filepath.Dir(fullPath), 0755)
	if err != nil {
		return err
	}

	f, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Warnf("unable to close file %s: %v", fullPath, err)
		}
	}()
	_, err = io.Copy(f, content)
	return err
}
