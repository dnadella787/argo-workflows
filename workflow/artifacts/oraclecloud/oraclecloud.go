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
	"io/fs"
	"k8s.io/utils/pointer"
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

func (ad *ArtifactDriver) Load(inputArtifact *v1alpha1.Artifact, localPath string) error {
	// get new OCI object storage client
	client, err := ad.newOracleCloudClient()
	if err != nil {
		return err
	}

	// get object storage namespace for the tenancy
	ns, err := getNamespace(client)
	if err != nil {

	}

	// path to object in object storage bucket -> {key}/path/to/artifact
	objPath := path.Join(inputArtifact.OracleCloud.Key, inputArtifact.Name)
	// local file path the file needs to be copied to
	fPath := path.Join(localPath, inputArtifact.Name)

	return ad.loadDirectory(client, ns, objPath, fPath)
}

func (ad *ArtifactDriver) OpenStream(a *v1alpha1.Artifact) (io.ReadCloser, error) {
	return nil, nil
}

func (ad *ArtifactDriver) Save(filePath string, outputArtifact *v1alpha1.Artifact) error {
	client, err := ad.newOracleCloudClient()
	if err != nil {
		return err
	}

	ns, err := getNamespace(client)
	if err != nil {
		return err
	}

	return ad.uploadDirectory(client, ns, outputArtifact.OracleCloud.Key, filePath)
}

func (ad *ArtifactDriver) Delete(artifact *v1alpha1.Artifact) error {
	return nil
}

func (ad *ArtifactDriver) ListObjects(artifact *v1alpha1.Artifact) ([]string, error) {
	return nil, nil
}

func (ad *ArtifactDriver) IsDirectory(artifact *v1alpha1.Artifact) (bool, error) {
	return true, nil
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

func getNamespace(client *objectstorage.ObjectStorageClient) (string, error) {
	ctx := context.Background()
	ns, err := client.GetNamespace(ctx, objectstorage.GetNamespaceRequest{})
	if err != nil {
		return "", err
	}
	return *ns.Value, err
}

func (ad *ArtifactDriver) uploadFile(client *objectstorage.ObjectStorageClient, namespace, objPath, fPath string, fSize int64) error {
	file, err := os.Open(fPath)
	if err != nil {
		return err
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Warnf("Unable to close file %s", fPath)
		}
	}()

	ctx := context.Background()
	_, err = client.PutObject(ctx, objectstorage.PutObjectRequest{
		NamespaceName: pointer.String(namespace),
		BucketName:    pointer.String(ad.bucketName),
		ObjectName:    pointer.String(objPath),
		PutObjectBody: file,
		ContentLength: pointer.Int64(fSize),
	})
	return err
}

func (ad *ArtifactDriver) uploadDirectory(client *objectstorage.ObjectStorageClient, namespace, objBase, dirBase string) error {
	return filepath.Walk(dirBase, func(fPath string, fs fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if fs.IsDir() {
			return nil
		}

		// Construct the object name in OCI Object Storage
		// objName = {artifact.oracleCloud.key}/{artifact.path}
		objName := path.Join(objBase, dirBase)
		return ad.uploadFile(client, namespace, objName, fPath, fs.Size())
	})
}

// loadFile downloads the contents of a specific file
// from object storage to local storage
func (ad *ArtifactDriver) loadFile(client *objectstorage.ObjectStorageClient, namespace, objPath, fPath string) error {
	content, err := ad.getObjectContent(client, namespace, objPath)
	if err == nil {
		return downloadObjectContent(content, fPath)
	}
	return err
}

// loadDirectory loads an entire directory but works for a single
// file too because the directory object storage prefix for a file
// is just the entire file name which gets returned in list call
func (ad *ArtifactDriver) loadDirectory(client *objectstorage.ObjectStorageClient, namespace, dirObjPrefix, localPath string) error {
	var nextStartsWith *string
	objsFound := false
	ctx := context.Background()

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
			err = ad.loadFile(client, namespace, *obj.Name, filePath)
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

// getObjectContent returns the content of a specific object in an OCI object storage bucket
func (ad *ArtifactDriver) getObjectContent(client *objectstorage.ObjectStorageClient, namespace, objPath string) (io.ReadCloser, error) {
	ctx := context.Background()
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

// downloadObjectContent downloads file content to local storage at fPath
func downloadObjectContent(content io.ReadCloser, fPath string) error {
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
