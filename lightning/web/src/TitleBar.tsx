// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import AppBar from '@material-ui/core/AppBar';
import { blueGrey, green, lime, red } from '@material-ui/core/colors';
import { createStyles, Theme, WithStyles, withStyles } from '@material-ui/core/styles';
import Toolbar from '@material-ui/core/Toolbar';
import * as React from 'react';

import * as api from './api';
import ErrorButton from './ErrorButton';
import InfoButton from './InfoButton';
import PauseButton from './PauseButton';
import RefreshButton from './RefreshButton';
import TaskButton from './TaskButton';
import TitleLink from './TitleLink';


interface Props extends WithStyles<typeof styles> {
    taskQueue: api.TaskQueue
    taskProgress: api.TaskProgress
    paused: boolean
    onRefresh: () => Promise<void>
    onSubmitTask: (taskCfg: string) => Promise<void>
    onTogglePaused: () => void
}

const styles = (theme: Theme) => createStyles({
    root: {
        flexGrow: 1,
    },
    title: {
        flexGrow: 1,
    },
    appBar: {
        transitionProperty: 'background-color',
        transitionDuration: '0.3s',
        zIndex: theme.zIndex.drawer + 1,
    },
    appBar_notStarted: {
        backgroundColor: blueGrey[700],
    },
    appBar_running: {
        backgroundColor: lime[700],
    },
    appBar_succeed: {
        backgroundColor: green[700],
    },
    appBar_failed: {
        backgroundColor: red[700],
    },
});

class TitleBar extends React.Component<Props> {
    render() {
        const { classes } = this.props;

        const appBarClass = classes.appBar + ' ' + api.classNameOfStatus(
            this.props.taskProgress,
            classes.appBar_notStarted,
            classes.appBar_running,
            classes.appBar_succeed,
            classes.appBar_failed,
        );

        return (
            <div className={classes.root}>
                <AppBar position='fixed' className={appBarClass}>
                    <Toolbar>
                        <TitleLink className={classes.title} />
                        {this.props.taskProgress.m &&
                            <ErrorButton lastError={this.props.taskProgress.m} color='inherit' />
                        }
                        <InfoButton taskQueue={this.props.taskQueue} />
                        <TaskButton onSubmitTask={this.props.onSubmitTask} />
                        <PauseButton paused={this.props.paused} onTogglePaused={this.props.onTogglePaused} />
                        <RefreshButton onRefresh={this.props.onRefresh} />
                    </Toolbar>
                </AppBar>
            </div>
        );
    }
}

export default withStyles(styles)(TitleBar);
