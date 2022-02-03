package ipfs

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-filestore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-ipfs/repo"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	mfs "github.com/ipfs/go-mfs"
	"github.com/ipfs/go-unixfs"
)

func NewRoot(ctx context.Context, repo repo.Repo, dag format.DAGService) (*mfs.Root, error) {
	dsk := datastore.NewKey("/local/filesroot")
	pf := func(ctx context.Context, c cid.Cid) error {
		rootDS := repo.Datastore()
		if err := rootDS.Sync(ctx, blockstore.BlockPrefix); err != nil {
			return err
		}
		if err := rootDS.Sync(ctx, filestore.FilestorePrefix); err != nil {
			return err
		}

		if err := rootDS.Put(ctx, dsk, c.Bytes()); err != nil {
			return err
		}
		return rootDS.Sync(ctx, dsk)
	}

	var nd *merkledag.ProtoNode
	val, err := repo.Datastore().Get(ctx, dsk)

	switch {
	case err == datastore.ErrNotFound || val == nil:
		nd = unixfs.EmptyDirNode()
		err := dag.Add(ctx, nd)
		if err != nil {
			return nil, fmt.Errorf("failure writing to dagstore: %s", err)
		}
	case err == nil:
		c, err := cid.Cast(val)
		if err != nil {
			return nil, err
		}

		rnd, err := dag.Get(ctx, c)
		if err != nil {
			return nil, fmt.Errorf("error loading filesroot from DAG: %s", err)
		}

		pbnd, ok := rnd.(*merkledag.ProtoNode)
		if !ok {
			return nil, merkledag.ErrNotProtobuf
		}

		nd = pbnd
	default:
		return nil, err
	}

	return mfs.NewRoot(ctx, dag, nd, pf)
}
