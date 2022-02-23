package resolve

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"github.com/operator-framework/api/pkg/operators/v1alpha1"
	"github.com/operator-framework/operator-registry/alpha/declcfg"
	orRegistry "github.com/operator-framework/operator-registry/pkg/registry"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"
)

type ManifestLoader interface {
	// todo: remove one-to-one url/manifest assumption
	LoadManifest(src *url.URL, dst *unstructured.Unstructured) error
}

type SchemelessLoader struct{}

func (l SchemelessLoader) LoadManifest(src *url.URL, dst *unstructured.Unstructured) error {
	abs, err := filepath.Abs(src.String())
	if err != nil {
		return fmt.Errorf("could not interpret schemeless argument as file path: %w", err)
	}
	src = &url.URL{
		Scheme: "file",
		Path:   abs,
	}
	return FileLoader{}.LoadManifest(src, dst)
}

type FileLoader struct{}

func (l FileLoader) LoadManifest(src *url.URL, dst *unstructured.Unstructured) error {
	fd, err := os.Open(src.Path)
	if err != nil {
		return fmt.Errorf("failed to open file %q: %w", src.Path, err)
	}
	defer fd.Close()

	dec := yaml.NewYAMLOrJSONDecoder(fd, 64)
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("failed to decode file %q: %w", src.Path, err)
	}

	return nil
}

type FileBasedCatalogLoader struct{}

func (fbcl FileBasedCatalogLoader) LoadManifest(src *url.URL) (*orRegistry.Querier, v1alpha1.Subscription, error) {
	fbc, err := declcfg.LoadFS(os.DirFS(src.Path))
	if err != nil {
		return nil, v1alpha1.Subscription{}, fmt.Errorf("error forming declarative config: %w", err)
	}

	m, err := declcfg.ConvertToModel(*fbc)
	if err != nil {
		return nil, v1alpha1.Subscription{}, fmt.Errorf("error converting fbc to model: %w", err)
	}

	sub, err := fbcl.buildSubscriptions(src.Query())
	if err != nil {
		return nil, v1alpha1.Subscription{}, err
	}

	querier, err := orRegistry.NewQuerier(m)
	return querier, sub, err
}

func (fbcl FileBasedCatalogLoader) buildSubscriptions(query url.Values) (v1alpha1.Subscription, error) {
	bundle := query.Get("bundle")
	channel := query.Get("channel")

	if bundle+channel == "" {
		return v1alpha1.Subscription{}, fmt.Errorf("must define a bundle, version and channel when using a file based catalog")
	}

	return v1alpha1.Subscription{
		Spec:       &v1alpha1.SubscriptionSpec{Package: bundle, Channel: channel},
		ObjectMeta: v1.ObjectMeta{Name: fmt.Sprintf("sample-%s-subscription", bundle)},
	}, nil
}
