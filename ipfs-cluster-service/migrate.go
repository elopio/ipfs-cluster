package main

import (
	"bufio"
	"cli"
	"errors"
	"fmt"
	"os"

	"github.com/ipfs/ipfs-cluster"
	"github.com/ipfs/ipfs-cluster/state/mapstate"
)


func upgrade(c *cli.Context) error {
	if c.NArg() < 1 || c.NArg() > 2 {
		return fmt.Errorf("Usage: <BACKUP-FILE-PATH> [RAFT-DATA-DIR]")
	}
	//Load configs                                                             
	cfg, clusterCfg, _, _, consensusCfg, _, _, _ := makeConfigs()
	err := cfg.LoadJSONFromFile(configPath)
	if err != nil {
		return errors.WithMessage(err, "loading config")
	}
	backupFilePath := c.Args().First()
	var raftDataPath string
	if c.NArg() == 1 {
		raftDataPath = consensusCfg.DataFolder
	} else {
		raftDataPath = c.Args().Get(1)
	}

	//Migrate backup to new state                                                              
	backup, err := os.Open(backupFilePath)
	if err != nil {
		return errors.WithMessage(err, "opening backup state file")
	}

	defer backup.Close()
	r := bufio.NewReader(backup)
	newState := mapstate.NewMapState()
	err = newState.Restore(r)
	if err != nil {
		return errors.WithMessage(err, "migrating state to newest version")
	}
	//Record peers of cluster                                                                  
	var peers []peer.ID
	for _, m := range clusterCfg.Peers {
		pid, _, err := ipfscluster.MultiaddrSplit(m)
		if err != nil {
			return errors.WithMessage(err, "parsing peer addrs in cluster-config")
		}
		peers = append(peers, pid)
	}
	peers = append(peers, clusterCfg.ID)
	//Reset raft state to a snapshot of the new migrated state                                 
	err = raft.SnapshotReset(*newState, consensusCfg, raftDataPath, peers)
	if err != nil {
		return errors.WithMessage(err, "migrating raft state to new format")
	}
	return nil
}


func needsUpdate(cfg *Config) bool {
	state := mapstate.NewMapState()
	r, err := raft.ExistingStateReader(cfg) // Note direct dependence on raft here
	if err == nil { //err != nil no snapshots so skip check
		storedV, err := state.Version(r)
		if storedV != state.Version || err != nil {
			logger.Error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                        logger.Error("Raft state is in a non-supported version")
			err = state.Restore(r)
			if err == nil {
				err = ipfscluster.BackupState(cfg, *state)
				if err == nil {
					logger.Error("An updated backup of this state has been saved")
					logger.Error("to baseDir/backups.  To setup state for use")
					logger.Error("run ipfs-cluster-service migration on the latest backup")
				}
				logger.Error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
				return true
			}
		}
	}
	return false
}
