package raft_test

import (
	"fmt"
	"log"
	"testing"
	"time"

	"google.golang.org/grpc/grpclog"

	"golang.org/x/net/context"

	"github.com/Sirupsen/logrus"
	"github.com/docker/swarmkit/api"
	cautils "github.com/docker/swarmkit/ca/testutils"
	"github.com/docker/swarmkit/manager/state/raft"
	raftutils "github.com/docker/swarmkit/manager/state/raft/testutils"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
)

var tc *cautils.TestCA

func init() {
	grpclog.SetLogger(log.New(ioutil.Discard, "", log.LstdFlags | log.Lshortfile))
	logrus.SetOutput(ioutil.Discard)
	//logrus.SetLevel(logrus.DebugLevel)

	tc = cautils.NewTestCA(nil, cautils.AcceptancePolicy(true, true, ""))
}

func TestStressLeaveJoin(t *testing.T) {
	t.Parallel()

	// Bring up a 3 nodes cluster
	nodes, clockSource := raftutils.NewRaftCluster(t, tc)
	defer raftutils.TeardownCluster(t, nodes)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	stop := make(chan struct{})
	defer close(stop)

	go func() {
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				clockSource.IncrementBySeconds(1)
			}
		}
	}()

	leaderNode := raftutils.Leader(nodes)
	leavedNodes := map[uint64]struct{}{}
	for i:=0; i < 1000; i++ {
		fmt.Println("iteration:", i)
		if len(nodes) >= 2 {
			// leader leave
			for id, node := range nodes {
				if node != leaderNode {
					continue
				}

				// send leave request
				client, err := leaderNode.ConnectToMember(leaderNode.Address, 0)
				assert.NoError(t, err)
				raftClient := api.NewRaftMembershipClient(client.Conn)

				fmt.Println("before leave")
				resp, err := raftClient.Leave(context.Background(), &api.LeaveRequest{Node: &api.RaftMember{RaftID: node.Config.ID}})
				fmt.Println("end leave")

				if err != nil {
					fmt.Println(resp, err)
				}
				raftutils.TeardownCluster(t, map[uint64]*raftutils.TestNode{id: nodes[id]})
				leavedNodes[id] = struct{}{}
				client.Conn.Close()
				delete(nodes, id)

				// leader is leaving, wait for a new election
				err = raftutils.WaitForCluster(t, clockSource, nodes)
				if err != nil {
					// fixme: if there are only 2 nodes in the cluster and leader is leaving
					// after committing raft conf change, leader may close connect before noticing follower this commit
					// As a result, follower still assumes there are 2 nodes on the cluster and can't get elected into
					// leader by acquiring votes from the majority (2 nodes).
					fmt.Println("wait error: ", err)
					for _, node := range nodes {
						// expected output: len(node.Node.GetMemberlist()) == 2
						// as explained above
						fmt.Println(node.Address, node.Node.Node.Status(), len(node.Node.GetMemberlist()))
					}
					t.Fatal(err)
				}
				leaderNode = raftutils.Leader(nodes)
				for _, node := range nodes {
					assert.Equal(t, len(nodes), len(node.GetMemberlist()))
				}

				break
			}

		} else {
			// join
			for id := range leavedNodes {
				nodes[id] = raftutils.NewNode(t, clockSource, tc, raft.NewNodeOptions{
					JoinAddr: leaderNode.Address})
				err := nodes[id].JoinAndStart()
				if err == nil {
					go nodes[id].Run(context.Background())
					err = raftutils.WaitForCluster(t, clockSource, nodes)
					if err != nil {
						// fixme: sometimes it takes a long time for a new cluster to be stable
						fmt.Println("wait error: ", err)
						for _, node := range nodes {
							fmt.Println(node.Address, node.Node.Node.Status(), node.Node.Node.Status().Applied, len(node.Node.GetMemberlist()))
						}
					}
					delete(leavedNodes, id)
				} else {
					raftutils.TeardownCluster(t, map[uint64]*raftutils.TestNode{id: nodes[id]})
				}
				break
			}
		}
	}
}
