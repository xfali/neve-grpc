/*
 * Copyright (C) 2022, Xiongfa Li.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nevegrpc

import (
	"github.com/xfali/fig"
	"github.com/xfali/neve-core/bean"
	"github.com/xfali/neve-grpc/logger"
	"github.com/xfali/neve-grpc/server"
	"github.com/xfali/xlog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

type processor struct {
	logger          xlog.Logger
	srvConf         *serverConf
	srv             *grpc.Server
	serversRegistry []server.RegistrarAware
}

type serverConf struct {
	Host         string `json:"host" yaml:"host"`
	Port         int    `json:"port" yaml:"port"`
	ReadTimeout  int    `json:"readTimeout" yaml:"readTimeout"`
	WriteTimeout int    `json:"writeTimeout" yaml:"writeTimeout"`

	Tls tlsConf `json:"tls" yaml:"tls"`
}

type tlsConf struct {
	Cert string `json:"cert" yaml:"cert"`
	Key  string `json:"key" yaml:"key"`
}

type Opt func(processor *processor)

func NewProcessor(opts ...Opt) *processor {
	ret := &processor{
		logger: xlog.GetLogger(),
	}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}

func (p *processor) Init(conf fig.Properties, container bean.Container) error {
	p.srvConf = &serverConf{}
	err := conf.GetValue("neve.grpc.server", p.srvConf)
	if err != nil {
		return err
	}
	grpclog.SetLoggerV2(logger.New(p.logger))
	return nil
}

func (p *processor) Classify(o interface{}) (bool, error) {
	switch v := o.(type) {
	case server.RegistrarAware:
		p.serversRegistry = append(p.serversRegistry, v)
		return true, nil
	}
	return false, nil
}

func (p *processor) Process() error {
	for _, sr := range p.serversRegistry {
		sr.RegisterService(p.srv)
	}
	return nil
}

func (p *processor) BeanDestroy() error {
	if p.srv != nil {
		p.srv.Stop()
	}
	return nil
}
