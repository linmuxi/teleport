/*
Copyright 2018-2019 Gravitational, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cache

import (
	"context"
	"sync"
	"time"

	"github.com/gravitational/teleport"
	"github.com/gravitational/teleport/lib/backend"
	"github.com/gravitational/teleport/lib/defaults"
	"github.com/gravitational/teleport/lib/services"
	"github.com/gravitational/teleport/lib/services/local"

	"github.com/gravitational/trace"
	log "github.com/sirupsen/logrus"
)

// Cache implements auth.AccessPoint interface and remembers
// the previously returned upstream value for each API call.
//
// This which can be used if the upstream AccessPoint goes offline
type Cache struct {
	sync.RWMutex
	Config
	*log.Entry
	ctx                context.Context
	cancel             context.CancelFunc
	trustCache         services.Trust
	clusterConfigCache services.ClusterConfiguration
	provisionerCache   services.Provisioner
}

// Config is Cache config
type Config struct {
	// Context is context for parent operations
	Context context.Context
	// Events provides events watchers
	Events services.Events
	// Trust is a service providing information about certificate
	// authorities
	Trust services.Trust
	// ClusterConfig is a cluster configuration service
	ClusterConfig services.ClusterConfiguration
	// Provisioner is a provisioning service
	Provisioner services.Provisioner
	// Backend is a backend for local cache
	Backend backend.Backend
	// RetryPeriod is a period between cache retries on failures
	RetryPeriod time.Duration
	// ReloadPeriod is a period when cache performs full reload
	ReloadPeriod time.Duration
	// EventsC is a channel for event notifications,
	// used in tests
	EventsC chan CacheEvent
}

// CheckAndSetDefaults checks parameters and sets default values
func (c *Config) CheckAndSetDefaults() error {
	if c.Context == nil {
		c.Context = context.Background()
	}
	if c.Events == nil {
		return trace.BadParameter("missing Events parameter")
	}
	if c.Trust == nil {
		return trace.BadParameter("missing Trust parameter")
	}
	if c.ClusterConfig == nil {
		return trace.BadParameter("missing ClusterConfig parameter")
	}
	if c.Provisioner == nil {
		return trace.BadParameter("missing Provisiong parameter")
	}
	if c.Backend == nil {
		return trace.BadParameter("missing Backend parameter")
	}
	if c.RetryPeriod == 0 {
		c.RetryPeriod = defaults.HighResPollingPeriod
	}
	if c.ReloadPeriod == 0 {
		c.ReloadPeriod = defaults.LowResPollingPeriod
	}
	return nil
}

// CacheEvent is event used in tests
type CacheEvent struct {
	// Type is event type
	Type string
	// Event is event processed
	// by the event cycle
	Event services.Event
}

const (
	// EventProcessed is emitted whenever event is processed
	EventProcessed = "event_processed"
	// WatcherStarted is emitted when a new event watcher is started
	WatcherStarted = "watcher_started"
)

// New creates a new instance of Cache
func New(config Config) (*Cache, error) {
	if err := config.CheckAndSetDefaults(); err != nil {
		return nil, trace.Wrap(err)
	}
	ctx, cancel := context.WithCancel(config.Context)
	cs := &Cache{
		ctx:                ctx,
		cancel:             cancel,
		Config:             config,
		trustCache:         local.NewCAService(config.Backend),
		clusterConfigCache: local.NewClusterConfigurationService(config.Backend),
		provisionerCache:   local.NewProvisioningService(config.Backend),
		Entry: log.WithFields(log.Fields{
			trace.Component: teleport.ComponentCachingClient,
		}),
	}
	_, err := cs.fetch()
	if err != nil {
		return nil, trace.Wrap(err)
	}
	go cs.update()
	return cs, nil
}

// instance is a cache instance,
type instance struct {
	parent *Cache
	services.Trust
}

func (c *Cache) update() {
	t := time.NewTicker(c.RetryPeriod)
	defer t.Stop()

	r := time.NewTicker(c.ReloadPeriod)
	defer r.Stop()
	for {
		select {
		case <-t.C:
		case <-r.C:
		case <-c.ctx.Done():
			return
		}
		err := c.fetchAndWatch()
		if err != nil {
			c.Warningf("Going to re-init the cache because of the error: %v.", err)
		}
	}
}

func (c *Cache) notify(event CacheEvent) {
	if c.EventsC == nil {
		return
	}
	select {
	case c.EventsC <- event:
		return
	case <-c.ctx.Done():
		return
	}
}

func (c *Cache) fetch() (int64, error) {
	id1, err := c.updateCertAuthorities(services.HostCA)
	if err != nil {
		return -1, trace.Wrap(err)
	}
	id2, err := c.updateCertAuthorities(services.UserCA)
	if err != nil {
		return -1, trace.Wrap(err)
	}
	id3, err := c.updateStaticTokens()
	if err != nil {
		return -1, trace.Wrap(err)
	}
	id4, err := c.updateTokens()
	if err != nil {
		return -1, trace.Wrap(err)
	}
	id5, err := c.updateClusterConfig()
	if err != nil {
		return -1, trace.Wrap(err)
	}
	id6, err := c.updateClusterName()
	if err != nil {
		return -1, trace.Wrap(err)
	}
	return max(id1, id2, id3, id4, id5, id6), nil
}

// Close closes all outstanding and active cache operations
func (c *Cache) Close() error {
	c.cancel()
	return nil
}

func (c *Cache) fetchAndWatch() error {
	watcher, err := c.Events.NewWatcher(c.ctx, services.Watch{
		Kinds: []services.WatchKind{
			{Kind: services.KindCertAuthority, LoadSecrets: true},
			{Kind: services.KindStaticTokens},
			{Kind: services.KindToken},
			{Kind: services.KindClusterName},
			{Kind: services.KindClusterConfig},
		},
	})
	if err != nil {
		return trace.Wrap(err)
	}
	defer watcher.Close()
	// before fetch, make sure watcher is synced by receiving init event,
	// to avoid the scenario:
	// 1. Cache process:   w = NewWatcher()
	// 2. Cache process:   c.fetch()
	// 3. Backend process: addItem()
	// 4. Cache process:   <- w.Events()
	//
	// If there is a way that NewWatcher() on line 1 could
	// return without subscription established first,
	// Code line 3 could execute and line 4 could miss event,
	// wrapping up with out of sync replica.
	// To avoid this, before doing fetch,
	// cache process makes sure the connection is established
	// by receiving init event first.
	select {
	case <-watcher.Done():
		if err != nil {
			return trace.Wrap(watcher.Error())
		}
		return trace.ConnectionProblem(nil, "unexpected watcher close")
	case <-c.ctx.Done():
		return trace.ConnectionProblem(c.ctx.Err(), "context is closing")
	case event := <-watcher.Events():
		if event.Type != backend.OpInit {
			return trace.BadParameter("expected init event, got %v instead", event.Type)
		}
	}
	resourceID, err := c.fetch()
	if err != nil {
		return trace.Wrap(err)
	}
	c.notify(CacheEvent{Type: WatcherStarted})
	for {
		select {
		case <-watcher.Done():
			if err != nil {
				return trace.Wrap(watcher.Error())
			}
			return trace.ConnectionProblem(nil, "unexpected watcher close")
		case <-c.ctx.Done():
			return trace.ConnectionProblem(c.ctx.Err(), "context is closing")
		case event := <-watcher.Events():
			resourceID, err = c.processEvent(event, resourceID)
			if err != nil {
				return trace.Wrap(err)
			}
			c.notify(CacheEvent{Event: event, Type: EventProcessed})
		}
	}
}

func (c *Cache) processEvent(event services.Event, resourceID int64) (int64, error) {
	switch event.Type {
	case backend.OpDelete:
		switch event.Resource.GetKind() {
		case services.KindToken:
			err := c.provisionerCache.DeleteToken(event.Resource.GetName())
			if err != nil {
				c.Warningf("Failed to delete provisioning token %v.", err)
				return -1, trace.Wrap(err)
			}
		case services.KindClusterConfig:
			err := c.clusterConfigCache.DeleteClusterConfig()
			if err != nil {
				c.Warningf("Failed to delete cluster config %v.", err)
				return -1, trace.Wrap(err)
			}
		case services.KindClusterName:
			err := c.clusterConfigCache.DeleteClusterName()
			if err != nil {
				c.Warningf("Failed to delete cluster name %v.", err)
				return -1, trace.Wrap(err)
			}
		case services.KindStaticTokens:
			err := c.clusterConfigCache.DeleteStaticTokens()
			if err != nil {
				c.Warningf("Failed to delete static tokens %v.", err)
				return -1, trace.Wrap(err)
			}
		case services.KindCertAuthority:
			err := c.trustCache.DeleteCertAuthority(services.CertAuthID{
				Type:       services.CertAuthType(event.Resource.GetSubKind()),
				DomainName: event.Resource.GetName(),
			})
			if err != nil {
				c.Warningf("Failed to delete cert authority %v.", err)
				return -1, trace.Wrap(err)
			}
		default:
			c.Debugf("Skipping unsupported resource %v", event.Resource.GetKind())
		}
	case backend.OpPut:
		if resourceID > event.Resource.GetResourceID() {
			c.Warningf("Ignoring out of order resource ID, last processed %v, received: %v.", resourceID, event.Resource.GetResourceID())
			return resourceID, nil
		}
		resourceID = event.Resource.GetResourceID()
		switch resource := event.Resource.(type) {
		case services.StaticTokens:
			if err := c.clusterConfigCache.SetStaticTokens(resource); err != nil {
				return -1, trace.Wrap(err)
			}
		case services.ProvisionToken:
			if err := c.provisionerCache.UpsertToken(resource); err != nil {
				return -1, trace.Wrap(err)
			}
		case services.CertAuthority:
			if err := c.trustCache.UpsertCertAuthority(resource); err != nil {
				return -1, trace.Wrap(err)
			}
		case services.ClusterConfig:
			if err := c.clusterConfigCache.SetClusterConfig(resource); err != nil {
				return -1, trace.Wrap(err)
			}
		case services.ClusterName:
			if err := c.clusterConfigCache.UpsertClusterName(resource); err != nil {
				return -1, trace.Wrap(err)
			}
		default:
			c.Warningf("Skipping unsupported resource %v.", event.Resource.GetKind())
		}
	default:
		c.Warningf("Skipping unsupported event type %v.", event.Type)
	}
	return resourceID, nil
}

func (c *Cache) updateCertAuthorities(caType services.CertAuthType) (int64, error) {
	authorities, err := c.Trust.GetCertAuthorities(caType, true, services.SkipValidation())
	if err != nil {
		return -1, trace.Wrap(err)
	}
	if err := c.trustCache.DeleteAllCertAuthorities(caType); err != nil {
		if !trace.IsNotFound(err) {
			return -1, trace.Wrap(err)
		}
	}
	var resourceID int64
	for _, ca := range authorities {
		if ca.GetResourceID() > resourceID {
			resourceID = ca.GetResourceID()
		}
		if err := c.trustCache.UpsertCertAuthority(ca); err != nil {
			return -1, trace.Wrap(err)
		}
	}
	return resourceID, nil
}

func (c *Cache) updateStaticTokens() (int64, error) {
	staticTokens, err := c.ClusterConfig.GetStaticTokens()
	if err != nil {
		if !trace.IsNotFound(err) {
			return -1, trace.Wrap(err)
		}
		err := c.clusterConfigCache.DeleteStaticTokens()
		if err != nil {
			if !trace.IsNotFound(err) {
				return -1, trace.Wrap(err)
			}
		}
		return 0, nil
	}
	err = c.clusterConfigCache.SetStaticTokens(staticTokens)
	if err != nil {
		return -1, trace.Wrap(err)
	}
	return staticTokens.GetResourceID(), nil
}

func (c *Cache) updateTokens() (int64, error) {
	tokens, err := c.Provisioner.GetTokens()
	if err != nil {
		return -1, trace.Wrap(err)
	}
	if err := c.provisionerCache.DeleteAllTokens(); err != nil {
		if !trace.IsNotFound(err) {
			return -1, trace.Wrap(err)
		}
	}
	var resourceID int64
	for _, token := range tokens {
		if token.GetResourceID() > resourceID {
			resourceID = token.GetResourceID()
		}
		if err := c.provisionerCache.UpsertToken(token); err != nil {
			return -1, trace.Wrap(err)
		}
	}
	return resourceID, nil
}

func (c *Cache) updateClusterConfig() (int64, error) {
	clusterConfig, err := c.ClusterConfig.GetClusterConfig()
	if err != nil {
		if !trace.IsNotFound(err) {
			return -1, trace.Wrap(err)
		}
		err := c.clusterConfigCache.DeleteClusterConfig()
		if err != nil {
			if !trace.IsNotFound(err) {
				return -1, trace.Wrap(err)
			}
		}
		return 0, nil
	}
	if err := c.clusterConfigCache.SetClusterConfig(clusterConfig); err != nil {
		if !trace.IsNotFound(err) {
			return -1, trace.Wrap(err)
		}
	}
	return clusterConfig.GetResourceID(), nil
}

func (c *Cache) updateClusterName() (int64, error) {
	clusterName, err := c.ClusterConfig.GetClusterName()
	if err != nil {
		if !trace.IsNotFound(err) {
			return -1, trace.Wrap(err)
		}
		err := c.clusterConfigCache.DeleteClusterName()
		if err != nil {
			if !trace.IsNotFound(err) {
				return -1, trace.Wrap(err)
			}
		}
		return 0, nil
	}
	if err := c.clusterConfigCache.UpsertClusterName(clusterName); err != nil {
		if !trace.IsNotFound(err) {
			return -1, trace.Wrap(err)
		}
	}
	return clusterName.GetResourceID(), nil
}

// GetCertAuthority returns certificate authority by given id. Parameter loadSigningKeys
// controls if signing keys are loaded
func (c *Cache) GetCertAuthority(id services.CertAuthID, loadSigningKeys bool, opts ...services.MarshalOption) (services.CertAuthority, error) {
	return c.trustCache.GetCertAuthority(id, loadSigningKeys, opts...)
}

// GetCertAuthorities returns a list of authorities of a given type
// loadSigningKeys controls whether signing keys should be loaded or not
func (c *Cache) GetCertAuthorities(caType services.CertAuthType, loadSigningKeys bool, opts ...services.MarshalOption) ([]services.CertAuthority, error) {
	return c.trustCache.GetCertAuthorities(caType, loadSigningKeys, opts...)
}

// GetStaticTokens gets the list of static tokens used to provision nodes.
func (c *Cache) GetStaticTokens() (services.StaticTokens, error) {
	return c.clusterConfigCache.GetStaticTokens()
}

// GetTokens returns all active (non-expired) provisioning tokens
func (c *Cache) GetTokens(opts ...services.MarshalOption) ([]services.ProvisionToken, error) {
	return c.provisionerCache.GetTokens(opts...)
}

// GetToken finds and returns token by ID
func (c *Cache) GetToken(token string) (services.ProvisionToken, error) {
	return c.provisionerCache.GetToken(token)
}

// GetClusterConfig gets services.ClusterConfig from the backend.
func (c *Cache) GetClusterConfig(opts ...services.MarshalOption) (services.ClusterConfig, error) {
	return c.clusterConfigCache.GetClusterConfig(opts...)
}

// GetClusterName gets the name of the cluster from the backend.
func (c *Cache) GetClusterName(opts ...services.MarshalOption) (services.ClusterName, error) {
	return c.clusterConfigCache.GetClusterName(opts...)
}

func max(v ...int64) int64 {
	var m int64
	for _, i := range v {
		if i > m {
			m = i
		}
	}
	return m
}
