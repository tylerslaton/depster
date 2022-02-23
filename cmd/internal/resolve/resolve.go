package resolve

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"

	"github.com/benluddy/depster/cmd/internal/commander"
	"github.com/operator-framework/api/pkg/operators/v1alpha1"
	"github.com/operator-framework/operator-lifecycle-manager/pkg/controller/registry"
	"github.com/operator-framework/operator-lifecycle-manager/pkg/controller/registry/resolver"
	"github.com/operator-framework/operator-lifecycle-manager/pkg/controller/registry/resolver/cache"
	"github.com/operator-framework/operator-registry/pkg/client"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func AddTo(c commander.Interface) {
	resolve := &cobra.Command{
		Use:   "resolve",
		Short: "Perform dependency resolution.",
		Args:  cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			log := logrus.New()
			log.SetOutput(ioutil.Discard)
			if verbose, err := cmd.Flags().GetBool("verbose"); err != nil {
				return fmt.Errorf("error reading flag: %w", err)
			} else if verbose {
				log.SetOutput(os.Stderr)
				log.SetLevel(logrus.DebugLevel)
			}

			var operators cache.OperatorSet
			fbc, err := cmd.Flags().GetBool("file-based-catalog")
			if err != nil {
				return err
			}

			if fbc {
				operators, err = resolveFileBasedCatalog(log, args)
				if err != nil {
					return fmt.Errorf("error while resolving file based catalog: %w", err)
				}
			} else {
				operators, err = resolveManifestsWithRunningCatalog(log, args)
				if err != nil {
					return fmt.Errorf("error while resolving manifests for running catalog: %w", err)
				}
			}

			printer := makeTabularPrinter(cmd.OutOrStdout())
			for _, each := range operators {
				printer.Print(each)
			}
			return printer.Close()
		},
	}
	resolve.PersistentFlags().BoolP("file-based-catalog", "f", false, "use to specify if reading a file based catalog")
	c.AddCommand(resolve)
}

func resolveFileBasedCatalog(log *logrus.Logger, args []string) (cache.OperatorSet, error) {
	fbcLoader := FileBasedCatalogLoader{}

	fbcURL, err := url.Parse(args[0])
	if err != nil {
		return nil, err
	}

	q, sub, err := fbcLoader.LoadManifest(fbcURL)
	if err != nil {
		return nil, fmt.Errorf("error while creating declarative config querier: %w", err)
	}

	p := phonyRegistryClientProvider{
		clients: make(map[registry.CatalogKey]client.Interface),
	}

	p.clients[registry.CatalogKey{}] = phonyClient{
		Querier: *q,
		oq:      *q,
	}

	sp := resolver.SourceProviderFromRegistryClientProvider(p, log)

	r := resolver.NewDefaultSatResolver(sp, phonyCatalogSourceLister{}, log)

	operators, err := r.SolveOperators(
		[]string{"test"},
		[]*v1alpha1.ClusterServiceVersion{},
		[]*v1alpha1.Subscription{&sub})
	if err != nil {
		return nil, fmt.Errorf("resolution failed: %w", err)
	}

	return operators, nil
}

func resolveManifestsWithRunningCatalog(log *logrus.Logger, args []string) (cache.OperatorSet, error) {
	var b InputBuilder
	for _, arg := range args {
		loaders := map[string]ManifestLoader{
			"":     SchemelessLoader{},
			"file": FileLoader{},
		}

		src, err := url.Parse(arg)
		if err != nil {
			return nil, fmt.Errorf("failed to parse argument %q as url: %w", arg, err)
		}

		loader, ok := loaders[src.Scheme]
		if !ok {
			return nil, fmt.Errorf("no manifest loader for scheme %q", src.Scheme)
		}

		var u unstructured.Unstructured
		if err := loader.LoadManifest(src, &u); err != nil {
			return nil, fmt.Errorf("error loading manifest from %q: %w", src, err)
		}

		if err := b.Add(&u); err != nil {
			return nil, fmt.Errorf("error adding input: %w", err)
		}
	}

	p := phonyRegistryClientProvider{
		clients: make(map[registry.CatalogKey]client.Interface),
	}

	for _, catalog := range b.catalogs {
		key := registry.CatalogKey{
			Namespace: catalog.GetNamespace(),
			Name:      catalog.GetName(),
		}
		if _, ok := p.clients[key]; ok {
			return nil, fmt.Errorf("duplicate catalog source: %s/%s", key.Namespace, key.Name)
		}
		if c, err := client.NewClient(catalog.Spec.Address); err != nil {
			return nil, fmt.Errorf("error creating registry client: %w", err)
		} else {
			p.clients[key] = c
		}

	}

	sp := resolver.SourceProviderFromRegistryClientProvider(p, log)

	r := resolver.NewDefaultSatResolver(sp, phonyCatalogSourceLister{}, log)

	var nsnames []string
	for _, ns := range b.namespaces {
		nsnames = append(nsnames, ns.GetName())
	}

	operators, err := r.SolveOperators(nsnames, b.csvs, b.subscriptions)
	if err != nil {
		return nil, fmt.Errorf("resolution failed: %w", err)
	}

	return operators, nil
}
