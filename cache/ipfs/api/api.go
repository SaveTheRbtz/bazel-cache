package api

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	config "github.com/ipfs/go-ipfs-config"
	"github.com/ipfs/go-path"
	"github.com/ipfs/go-unixfs"
	icore "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipfs/interface-go-ipfs-core/options"

	"github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/core/coreapi"
	"github.com/ipfs/go-ipfs/core/node/libp2p"
	"github.com/ipfs/go-ipfs/repo"
	"github.com/ipfs/go-ipfs/repo/fsrepo"

	"github.com/ipfs/go-ipfs/plugin/loader"

	"github.com/mitchellh/go-homedir"
)

const DefaultPathRoot = "~/.ipfs-bazel-cache"

func setupPlugins(externalPluginsPath string) error {
	plugins, err := loader.NewPluginLoader(filepath.Join(externalPluginsPath, "plugins"))
	if err != nil {
		return fmt.Errorf("error loading plugins: %s", err)
	}

	if err := plugins.Initialize(); err != nil {
		return fmt.Errorf("error initializing plugins: %s", err)
	}

	if err := plugins.Inject(); err != nil {
		return fmt.Errorf("error initializing plugins: %s", err)
	}

	return nil
}

func New(ctx context.Context, repoRoot string) (icore.CoreAPI, repo.Repo, error) {
	dir, err := homedir.Expand(repoRoot)
	if err != nil {
		return nil, nil, err
	}

	_ = setupPlugins(dir)

	if !fsrepo.IsInitialized(dir) {
		err = initRepo(dir)
		if err != nil {
			return nil, nil, err
		}
	}

	repo, err := fsrepo.Open(dir)
	if err != nil {
		return nil, nil, err
	}

	nodeOptions := &core.BuildCfg{
		Permanent: true,
		Online:    true,
		Routing:   libp2p.DHTOption,
		Repo:      repo,
	}

	node, err := core.NewNode(ctx, nodeOptions)
	if err != nil {
		return nil, nil, err
	}

	api, err := coreapi.NewCoreAPI(node)
	return api, repo, err
}

func initRepo(repoRoot string) error {
	identity, err := config.CreateIdentity(os.Stdout, []options.KeyGenerateOption{
		options.Key.Type(options.Ed25519Key),
	})
	if err != nil {
		return err
	}
	conf, err := config.InitWithIdentity(identity)

	if err := fsrepo.Init(repoRoot, conf); err != nil {
		return fmt.Errorf("fsrepo init failed: %s: %s", repoRoot, err)
	}

	return initializeIpnsKeyspace(repoRoot)
}

func initializeIpnsKeyspace(repoRoot string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r, err := fsrepo.Open(repoRoot)
	if err != nil { // NB: repo is owned by the node
		return err
	}

	nd, err := core.NewNode(ctx, &core.BuildCfg{Repo: r})
	if err != nil {
		return err
	}
	defer nd.Close()

	emptyDir := unixfs.EmptyDirNode()

	// pin recursively because this might already be pinned
	// and doing a direct pin would throw an error in that case
	err = nd.Pinning.Pin(ctx, emptyDir, true)
	if err != nil {
		return err
	}

	err = nd.Pinning.Flush(ctx)
	if err != nil {
		return err
	}

	return nd.Namesys.Publish(ctx, nd.PrivateKey, path.FromCid(emptyDir.Cid()))
}
