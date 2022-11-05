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

package logger

import "github.com/xfali/xlog"

type logger struct {
	log xlog.Logger
}

func New(log xlog.Logger) *logger {
	return &logger{
		log: log,
	}
}

// Info logs to INFO log. Arguments are handled in the manner of fmt.Print.
func (l *logger) Info(args ...interface{}) {
	l.log.Info(args...)
}

// Infoln logs to INFO log. Arguments are handled in the manner of fmt.Println.
func (l *logger) Infoln(args ...interface{}) {
	l.log.Infoln(args...)
}

// Infof logs to INFO log. Arguments are handled in the manner of fmt.Printf.
func (l *logger) Infof(format string, args ...interface{}) {
	l.log.Infof(format, args...)
}

// Warning logs to WARNING log. Arguments are handled in the manner of fmt.Print.
func (l *logger) Warning(args ...interface{}) {
	l.log.Warn(args...)
}

// Warningln logs to WARNING log. Arguments are handled in the manner of fmt.Println.
func (l *logger) Warningln(args ...interface{}) {
	l.log.Warnln(args...)
}

// Warningf logs to WARNING log. Arguments are handled in the manner of fmt.Printf.
func (l *logger) Warningf(format string, args ...interface{}) {
	l.log.Warnf(format, args...)
}

// Error logs to ERROR log. Arguments are handled in the manner of fmt.Print.
func (l *logger) Error(args ...interface{}) {
	l.log.Error(args...)
}

// Errorln logs to ERROR log. Arguments are handled in the manner of fmt.Println.
func (l *logger) Errorln(args ...interface{}) {
	l.log.Errorln(args...)
}

// Errorf logs to ERROR log. Arguments are handled in the manner of fmt.Printf.
func (l *logger) Errorf(format string, args ...interface{}) {
	l.log.Errorf(format, args...)
}

// Fatal logs to ERROR log. Arguments are handled in the manner of fmt.Print.
// gRPC ensures that all Fatal logs will exit with os.Exit(1).
// Implementations may also call os.Exit() with a non-zero exit code.
func (l *logger) Fatal(args ...interface{}) {
	l.log.Fatal(args...)
}

// Fatalln logs to ERROR log. Arguments are handled in the manner of fmt.Println.
// gRPC ensures that all Fatal logs will exit with os.Exit(1).
// Implementations may also call os.Exit() with a non-zero exit code.
func (l *logger) Fatalln(args ...interface{}) {
	l.log.Fatalln(args...)
}

// Fatalf logs to ERROR log. Arguments are handled in the manner of fmt.Printf.
// gRPC ensures that all Fatal logs will exit with os.Exit(1).
// Implementations may also call os.Exit() with a non-zero exit code.
func (l *logger) Fatalf(format string, args ...interface{}) {
	l.log.Fatalf(format, args...)
}

// V reports whether verbosity level l is at least the requested verbose level.
func (l *logger) V(lv int) bool {
	switch lv {
	case 2:
		return l.log.InfoEnabled()
	case 3:
		return l.log.WarnEnabled()
	case 4:
		return l.log.ErrorEnabled()
	case 5:
		return l.log.FatalEnabled()
	}
	return true
}
