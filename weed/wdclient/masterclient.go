package wdclient

import (
	"context"
	"math/rand"
	"time"

	"github.com/chrislusf/seaweedfs/weed/util"
	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
)

type MasterClient struct {
	clientType     string
	clientHost     pb.ServerAddress
	currentMaster  pb.ServerAddress
	masters        []pb.ServerAddress
	grpcDialOption grpc.DialOption

	vidMap

	OnPeerUpdate func(update *master_pb.ClusterNodeUpdate)
}

func NewMasterClient(grpcDialOption grpc.DialOption, clientType string, clientHost pb.ServerAddress, clientDataCenter string, masters []pb.ServerAddress) *MasterClient {
	return &MasterClient{
		clientType:     clientType,
		clientHost:     clientHost,
		masters:        masters,
		grpcDialOption: grpcDialOption,
		vidMap:         newVidMap(clientDataCenter),
	}
}

func (mc *MasterClient) GetMaster() pb.ServerAddress {
	mc.WaitUntilConnected()
	return mc.currentMaster
}

func (mc *MasterClient) WaitUntilConnected() {
	for mc.currentMaster == "" {
		time.Sleep(time.Duration(rand.Int31n(200)) * time.Millisecond)
	}
}

func (mc *MasterClient) KeepConnectedToMaster() {
	glog.V(1).Infof("%s masterClient bootstraps with masters %v", mc.clientType, mc.masters)
	for {
		mc.tryAllMasters()
		time.Sleep(time.Second)
	}
}

func (mc *MasterClient) FindLeaderFromOtherPeers(myMasterAddress pb.ServerAddress) (leader string) {
	for _, master := range mc.masters {
		if master == myMasterAddress {
			continue
		}
		if grpcErr := pb.WithMasterClient(false, master, mc.grpcDialOption, func(client master_pb.SeaweedClient) error {
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
			defer cancel()
			// 获取当前master集群的配置
			resp, err := client.GetMasterConfiguration(ctx, &master_pb.GetMasterConfigurationRequest{})
			if err != nil {
				return err
			}
			leader = resp.Leader
			return nil
		}); grpcErr != nil {
			glog.V(0).Infof("connect to %s: %v", master, grpcErr)
		}
		if leader != "" {
			glog.V(0).Infof("existing leader is %s", leader)
			return
		}
	}
	glog.V(0).Infof("No existing leader found!")
	return
}

// 尝试连接所有master直到找到leader
func (mc *MasterClient) tryAllMasters() {
	var nextHintedLeader pb.ServerAddress
	for _, master := range mc.masters {

		// 第一次可能去一个非leader的master，它会返回leader master
		nextHintedLeader = mc.tryConnectToMaster(master)
		// 找到或切换leader master
		for nextHintedLeader != "" {
			nextHintedLeader = mc.tryConnectToMaster(nextHintedLeader)
		}

		// 退出连接，清空状态
		mc.currentMaster = ""
		mc.vidMap = newVidMap("")
	}
}

// 连接到master循环更新volume id到location的映射
func (mc *MasterClient) tryConnectToMaster(master pb.ServerAddress) (nextHintedLeader pb.ServerAddress) {
	glog.V(1).Infof("%s masterClient Connecting to master %v", mc.clientType, master)
	gprcErr := pb.WithMasterClient(true, master, mc.grpcDialOption, func(client master_pb.SeaweedClient) error {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// stream 是 seaweedKeepConnectedClient
		// 向leader master发起请求,接收哪些节点有哪些volume变动
		// 另外，每个volumeServer会向leader master发送heartbeat以报告它所处节点有哪些 volume 变更了
		stream, err := client.KeepConnected(ctx)
		if err != nil {
			glog.V(1).Infof("%s masterClient failed to keep connected to %s: %v", mc.clientType, master, err)
			return err
		}

		// 先向leader发送 KeepConnectedRequest
		if err = stream.Send(&master_pb.KeepConnectedRequest{
			ClientType:    mc.clientType,
			ClientAddress: string(mc.clientHost),
			Version:       util.Version(),
		}); err != nil {
			glog.V(0).Infof("%s masterClient failed to send to %s: %v", mc.clientType, master, err)
			return err
		}

		glog.V(1).Infof("%s masterClient Connected to %v", mc.clientType, master)
		mc.currentMaster = master

		for {
			// 不断从stream接收消息
			resp, err := stream.Recv()
			if err != nil {
				glog.V(0).Infof("%s masterClient failed to receive from %s: %v", mc.clientType, master, err)
				return err
			}

			if resp.VolumeLocation != nil {
				// maybe the leader is changed
				if resp.VolumeLocation.Leader != "" {
					glog.V(0).Infof("redirected to leader %v", resp.VolumeLocation.Leader)
					nextHintedLeader = pb.ServerAddress(resp.VolumeLocation.Leader)
					return nil
				}

				// process new volume location
				// 记录leader master中收到和保存到的的volume id和对应的位置location url
				loc := Location{
					Url:        resp.VolumeLocation.Url,
					PublicUrl:  resp.VolumeLocation.PublicUrl,
					DataCenter: resp.VolumeLocation.DataCenter,
					GrpcPort:   int(resp.VolumeLocation.GrpcPort),
				}
				// 保存新增的volume数据到本地内存中
				for _, newVid := range resp.VolumeLocation.NewVids {
					glog.V(1).Infof("%s: %s masterClient adds volume %d", mc.clientType, loc.Url, newVid)
					// 保存到MasterClient的vidMap中
					mc.addLocation(newVid, loc)
				}
				// 保存删除的volume数据到本地内存中
				for _, deletedVid := range resp.VolumeLocation.DeletedVids {
					glog.V(1).Infof("%s: %s masterClient removes volume %d", mc.clientType, loc.Url, deletedVid)
					mc.deleteLocation(deletedVid, loc)
				}
			}

			if resp.ClusterNodeUpdate != nil {
				update := resp.ClusterNodeUpdate
				if mc.OnPeerUpdate != nil {
					if update.IsAdd {
						glog.V(0).Infof("+ %s %s leader:%v\n", update.NodeType, update.Address, update.IsLeader)
					} else {
						glog.V(0).Infof("- %s %s leader:%v\n", update.NodeType, update.Address, update.IsLeader)
					}
					mc.OnPeerUpdate(update)
				}
			}

		}

	})
	if gprcErr != nil {
		glog.V(1).Infof("%s masterClient failed to connect with master %v: %v", mc.clientType, master, gprcErr)
	}
	return
}

func (mc *MasterClient) WithClient(streamingMode bool, fn func(client master_pb.SeaweedClient) error) error {
	return util.Retry("master grpc", func() error {
		for mc.currentMaster == "" {
			time.Sleep(3 * time.Second)
		}
		return pb.WithMasterClient(streamingMode, mc.currentMaster, mc.grpcDialOption, func(client master_pb.SeaweedClient) error {
			return fn(client)
		})
	})
}
