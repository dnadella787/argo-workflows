package oraclecloud

import (
	"context"
	"fmt"
	"github.com/argoproj/argo-workflows/v3/errors"
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
	"strings"
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
	fPath := path.Join(localPath, inputArtifact.Name)
	// Download object directly assuming artifact is a single file
	err = ad.downloadObjectContent(ctx, client, *ns.Value, objPath, fPath)
	if err != nil {
		// the GetObject returned 404 so try to download the entire directory
		if e, ok := err.(ocicommons.ServiceError); ok && e.GetHTTPStatusCode() == http.StatusNotFound {
			return ad.loadDirectory(ctx, client, *ns.Value, objPath, fPath)
		}
		return err
	}
	return nil
}

func (ad *ArtifactDriver) downloadObjectContent(ctx context.Context, client *objectstorage.ObjectStorageClient, namespace, objPath, fPath string) error {
	content, err := ad.getObjectContent(ctx, client, namespace, objPath)
	if err == nil {
		// copy the file to local storage
		return downloadFile(content, fPath)
	}
	return err
}

func (ad *ArtifactDriver) getObjectContent(ctx context.Context, client *objectstorage.ObjectStorageClient, namespace, objPath string) (io.ReadCloser, error) {
	object, err := client.GetObject(ctx, objectstorage.GetObjectRequest{
		NamespaceName: pointer.String(namespace),
		BucketName:    pointer.String(ad.bucketName),
		ObjectName:    pointer.String(objPath),
	})
	if err != nil {
		return nil, err
	}
	return object.Content, nil
}

func downloadFile(content io.ReadCloser, fPath string) error {
	err := os.MkdirAll(filepath.Dir(fPath), 0755)
	if err != nil {
		return err
	}

	f, err := os.Create(fPath)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Warnf("unable to close file %s: %v", fPath, err)
		}
	}()
	_, err = io.Copy(f, content)
	return err
}

func (ad *ArtifactDriver) loadDirectory(ctx context.Context, client *objectstorage.ObjectStorageClient, namespace, dirObjPrefix, localPath string) error {
	var nextStartsWith *string
	objsFound := false
	for {
		objs, err := client.ListObjects(ctx, objectstorage.ListObjectsRequest{
			NamespaceName: pointer.String(namespace),
			BucketName:    pointer.String(ad.bucketName),
			Prefix:        pointer.String(dirObjPrefix),
			StartAfter:    nextStartsWith,
		})
		if err != nil {
			return err
		}
		if len(objs.Objects) > 0 {
			objsFound = true
		}
		nextStartsWith = objs.NextStartWith

		for _, obj := range objs.Objects {
			// remove the key + directory from the full object name and append it
			// to the local directory path
			filePath := path.Join(localPath, strings.TrimPrefix(*obj.Name, dirObjPrefix))
			err = ad.downloadObjectContent(ctx, client, namespace, *obj.Name, filePath)
			if err != nil {
				return err
			}
		}

		// paginated through all files
		if nextStartsWith == nil {
			break
		}
	}
	if !objsFound {
		return errors.New(errors.CodeNotFound, fmt.Sprintf("directory %s not found in Oracle Object Storage bucket %s in namespace %s", dirObjPrefix, ad.bucketName, namespace))
	}
	return nil
}
