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

import { List, ListItem, ListItemSecondaryAction, ListItemText, ListSubheader } from '@material-ui/core';
import Drawer from '@material-ui/core/Drawer';
import { createStyles, Theme, WithStyles, withStyles } from '@material-ui/core/styles';
import Typography from '@material-ui/core/Typography';
import * as JSONBigInt from 'json-bigint';
import * as React from 'react';

import * as api from './api';
import MoveTaskButton from './MoveTaskButton';


const drawerWidth = 180;

const styles = (theme: Theme) => createStyles({
    toolbar: theme.mixins.toolbar,
    drawer: {
        width: drawerWidth,
        flexShrink: 0,
    },
    drawerPaper: {
        width: drawerWidth,
    },
    content: {
        flexGrow: 1,
        padding: theme.spacing(3),
        marginLeft: drawerWidth,
        whiteSpace: 'pre',
    },
});

interface Props extends WithStyles<typeof styles> {
    taskQueue: api.TaskQueue
    getTaskCfg: (taskID: api.TaskID) => Promise<any>,
    onDelete: (taskID: api.TaskID) => void,
    onMoveToFront: (taskID: api.TaskID) => void,
    onMoveToBack: (taskID: api.TaskID) => void,
}

interface States {
    isLoading: boolean,
    taskCfg: any,
}

class InfoPage extends React.Component<Props, States> {
    constructor(props: Props) {
        super(props);

        this.state = {
            isLoading: false,
            taskCfg: null,
        };
    }

    async handleSelectTaskID(taskID: api.TaskID) {
        this.setState({ isLoading: true });
        const taskCfg = await this.props.getTaskCfg(taskID);
        this.setState({ isLoading: false, taskCfg });
    }

    async componentDidMount() {
        if (this.props.taskQueue.current !== null) {
            await this.handleSelectTaskID(this.props.taskQueue.current);
        }
    }

    renderListItem(taskID: api.TaskID, movable: boolean) {
        const date = api.dateFromTaskID(taskID)
        return (
            <ListItem button key={'' + taskID} onClick={() => this.handleSelectTaskID(taskID)} disabled={this.state.isLoading}>
                <ListItemText primary={date.toLocaleDateString()} secondary={date.toLocaleTimeString()} />
                <ListItemSecondaryAction>
                    <MoveTaskButton taskID={taskID} movable={movable} {... this.props} />
                </ListItemSecondaryAction>
            </ListItem>
        );
    }

    render() {
        const { classes } = this.props;
        return (
            <div>
                <Drawer className={classes.drawer} classes={{ paper: classes.drawerPaper }} open variant='permanent'>
                    <div className={classes.toolbar} />
                    <List>
                        <ListSubheader>Current</ListSubheader>
                        {this.props.taskQueue.current !== null && this.renderListItem(this.props.taskQueue.current, false)}
                        <ListSubheader>Queue</ListSubheader>
                        {this.props.taskQueue.queue.map(n => this.renderListItem(n, true))}
                    </List>
                </Drawer>
                <Typography className={classes.content} paragraph>
                    {JSONBigInt.stringify(this.state.taskCfg, undefined, 2)}
                </Typography>
            </div>
        )
    }
}

export default withStyles(styles)(InfoPage);
