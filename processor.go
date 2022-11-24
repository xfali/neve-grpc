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
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/xfali/fig"
	"github.com/xfali/goutils/idUtil"
	"github.com/xfali/neve-core/bean"
	"github.com/xfali/neve-grpc/logger"
	"github.com/xfali/neve-grpc/server"
	"github.com/xfali/xlog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"
	"net"
)

type marshalFunc func(v interface{}) ([]byte, error)

type processor struct {
	logger xlog.Logger

	srvConf *serverConf
	srv     *grpc.Server
	cert    *tls.Certificate

	marshalFunc     marshalFunc
	recoveryHandler func(err error) error

	serversRegistry []server.RegistrarAware
}

type serverConf struct {
	Host         string `json:"host" yaml:"host"`
	Port         int    `json:"port" yaml:"port"`
	ReadTimeout  int    `json:"readTimeout" yaml:"readTimeout"`
	WriteTimeout int    `json:"writeTimeout" yaml:"writeTimeout"`

	Tls tlsConf `json:"tls" yaml:"tls"`
	Log logConf `json:"log" yaml:"log"`
}

type tlsConf struct {
	Cert string `json:"cert" yaml:"cert"`
	Key  string `json:"key" yaml:"key"`
}

type logConf struct {
	Disable bool `json:"disable" yaml:"disable"`
}

type Opt func(processor *processor)

func NewProcessor(opts ...Opt) *processor {
	ret := &processor{
		logger:      xlog.GetLogger(),
		marshalFunc: json.Marshal,
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
	err := p.processServer()
	if err != nil {
		p.logger.Errorln(err)
		return err
	}
	return p.processClient()
}

func (p *processor) processClient() error {
	return nil
}

func (p *processor) processServer() error {
	if p.srvConf != nil && p.srvConf.Port != 0 {
		lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d",
			p.srvConf.Host, p.srvConf.Port))
		if err != nil {
			return err
		}

		var creds credentials.TransportCredentials
		if p.srvConf.Tls.Key != "" && p.srvConf.Tls.Cert != "" {
			creds, err = credentials.NewServerTLSFromFile(
				p.srvConf.Tls.Cert,
				p.srvConf.Tls.Key)
			if err != nil {
				return err
			}
		} else {
			if p.cert != nil {
				creds = credentials.NewServerTLSFromCert(p.cert)
			}
		}
		var opts grpc.ServerOption
		if !p.srvConf.Log.Disable {
			opts = grpc.ChainUnaryInterceptor(p.recoveryFunc, p.loggingFunc)
		} else {
			opts = grpc.ChainUnaryInterceptor(p.recoveryFunc)
		}
		if creds != nil {
			p.srv = grpc.NewServer(
				grpc.Creds(creds),
				opts)
		} else {
			p.srv = grpc.NewServer(opts)
		}

		for _, sr := range p.serversRegistry {
			sr.RegisterService(p.srv)
		}

		go func() {
			p.logger.Warnln(p.srv.Serve(lis))
		}()
	} else {
		p.logger.Warnln("neve grpc run without server. ")
	}
	return nil
}

func (p *processor) BeanDestroy() error {
	if p.srv != nil {
		p.srv.Stop()
	}
	return nil
}

func (p *processor) recoveryFunc(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler) (resp interface{}, err error) {
	defer func() {
		if o := recover(); o != nil {
			if v, ok := o.(error); ok {
				err = v
			} else {
				err = fmt.Errorf("%v", o)
			}
			if p.recoveryHandler != nil {
				err = p.recoveryHandler(err)
			} else {
				p.logger.Errorln(err)
			}
		}
	}()
	return handler(ctx, req)
}

func (p *processor) loggingFunc(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler) (resp interface{}, err error) {
	reqV := defaultMarshal(req, p.marshalFunc)
	id := idUtil.RandomId(32)
	p.logger.Infof("Grpc server Request[%s]: %s request: %s\n",
		id, info.FullMethod, reqV)
	resp, err = handler(ctx, req)
	if err != nil {
		p.logger.Infof("Grpc server Response[%s]: %s err: %s response: %s\n")
	} else {
		respV := defaultMarshal(resp, p.marshalFunc)
		p.logger.Infof("Grpc server Response[%s]: %s response: %s\n",
			id, info.FullMethod, respV)
	}
	return resp, err
}

func defaultMarshal(v interface{}, f marshalFunc) string {
	d, err := f(v)
	if err == nil {
		return string(d)
	}
	return fmt.Sprintf("%v", v)
}

type svrOpts struct {
}

var ServerOpts svrOpts

func (o svrOpts) RecoveryHandler(h func(error) error) Opt {
	return func(processor *processor) {
		processor.recoveryHandler = h
	}
}
