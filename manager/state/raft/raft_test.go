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
				client, err := leaderNode.ConnectToMember(leaderNode.Address, 10 * time.Second)
				assert.NoError(t, err)
				raftClient := api.NewRaftMembershipClient(client.Conn)

				ctx, _ := context.WithTimeout(context.Background(), 10 * time.Second)
				resp, err := raftClient.Leave(ctx, &api.LeaveRequest{Node: &api.RaftMember{RaftID: node.Config.ID}})

				assert.NoError(t, err, "error sending message to leave the raft")
				assert.NotNil(t, resp, "leave response message is nil")

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
						// fixme: it may take a long time for a new cluster to be stable, due the live lock issue of raft
						// see: https://github.com/coreos/etcd/issues/5679
						fmt.Println("wait error: ", err)
						for _, node := range nodes {
							fmt.Println(node.Address, node.Node.Node.Status(), node.Node.Node.Status().Applied, len(node.Node.GetMemberlist()))
						}
						t.Fatal(err)
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
