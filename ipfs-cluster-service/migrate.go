package main

import (
	"bufio"

	"fmt"
	"os"

	"github.com/urfave/cli"

	peer "github.com/libp2p/go-libp2p-peer"
	
	ipfscluster "github.com/ipfs/ipfs-cluster"
	"github.com/ipfs/ipfs-cluster/state/mapstate"
	"github.com/ipfs/ipfs-cluster/consensus/raft"
)


func upgrade() error {
	//Load configs                                                             
	cfg, clusterCfg, _, _, consensusCfg, _, _, _ := makeConfigs()
	err := cfg.LoadJSONFromFile(configPath)
	if err != nil {
		return err
	}

	newState := mapstate.NewMapState()

	//Reset raft state to a snapshot of the new migrated state                                 
	err = raft.SnapshotMigrate(consensusCfg, newState)
	if err != nil {
		return err
	}
	return nil
}


func validateVersion(cfg *ipfscluster.Config, cCfg *raft.Config) error {
	state := mapstate.NewMapState()
	validSnap, err := raft.LastState(cCfg, state) // Note direct dependence on raft here
	if !validSnap && err != nil {
		logger.Error("Error before reading last snapshot.")
		return err
	} else if validSnap && err != nil {
		logger.Error("Error after reading last snapshot. Snapshot potentially corrupt.")
		return err
	} else if validSnap && err == nil {
		if state.GetVersion() != state.Version {
			logger.Error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
			logger.Error("Out of date ipfs-cluster state is saved.")
			logger.Error("To migrate to the new version, run ipfs-cluster-service state upgrade.")
			logger.Error("To launch a node without this state, rename the consensus data directory.")
			logger.Error("Hint, the default is .ipfs-cluster/ipfs-cluster-data.")
			logger.Error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
			return errors.New("Outdated state version stored")
		}
	} // !validSnap && err != nil // no existing state, no check needed
	return nil
}
