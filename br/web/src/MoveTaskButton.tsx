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
// See the License for the specific language governing permissions and
// limitations under the License.

import IconButton from '@material-ui/core/IconButton';
import ListItemIcon from '@material-ui/core/ListItemIcon';
import ListItemText from '@material-ui/core/ListItemText';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';
import MenuList from '@material-ui/core/MenuList';
import ArrowDownwardIcon from '@material-ui/icons/ArrowDownward';
import ArrowUpwardIcon from '@material-ui/icons/ArrowUpward';
import CancelIcon from '@material-ui/icons/Cancel';
import MortVertIcon from '@material-ui/icons/MoreVert';
import * as React from 'react';

import * as api from './api';


interface Props {
    taskID: api.TaskID
    movable: boolean

    onDelete: (taskID: api.TaskID) => void
    onMoveToFront: (taskID: api.TaskID) => void
    onMoveToBack: (taskID: api.TaskID) => void
}

interface States {
    menuOpened: boolean
}

export default class MoveTaskButton extends React.Component<Props, States> {
    private ref: React.RefObject<HTMLButtonElement>;

    constructor(props: Props) {
        super(props);

        this.ref = React.createRef();

        this.state = {
            menuOpened: false,
        };
    }

    handleToggleMenu = () => {
        this.setState(state => ({ menuOpened: !state.menuOpened }));
    };

    handleCloseMenu = () => {
        this.setState({ menuOpened: false });
    };

    handleStopTask = () => {
        const taskID = this.props.taskID;
        const readableID = api.dateFromTaskID(taskID).toLocaleString();
        if (confirm(`Do you really want to stop and delete task queued at ${readableID}?`)) {
            this.props.onDelete(taskID);
            this.handleCloseMenu();
        }
    };

    handleMoveTaskToFront = () => {
        this.props.onMoveToFront(this.props.taskID);
        this.handleCloseMenu();
    };

    handleMoveTaskToBack = () => {
        this.props.onMoveToBack(this.props.taskID);
        this.handleCloseMenu();
    };

    render() {
        return (
            <div>
                <IconButton edge='end' ref={this.ref} onClick={this.handleToggleMenu}>
                    <MortVertIcon />
                </IconButton>
                <Menu anchorEl={this.ref.current} open={this.state.menuOpened} onClose={this.handleCloseMenu}>
                    <MenuList>
                        <MenuItem onClick={this.handleStopTask} divider={this.props.movable}>
                            <ListItemIcon>
                                <CancelIcon color='secondary' />
                            </ListItemIcon>
                            <ListItemText primaryTypographyProps={{ color: 'secondary' }}>
                                {this.props.movable ? 'Delete' : 'Stop'}
                            </ListItemText>
                        </MenuItem>
                        {this.props.movable && (
                            <>
                                <MenuItem onClick={this.handleMoveTaskToFront}>
                                    <ListItemIcon>
                                        <ArrowUpwardIcon />
                                    </ListItemIcon>
                                    <ListItemText>
                                        Move to front
                                    </ListItemText>
                                </MenuItem>
                                <MenuItem onClick={this.handleMoveTaskToBack}>
                                    <ListItemIcon>
                                        <ArrowDownwardIcon />
                                    </ListItemIcon>
                                    <ListItemText>
                                        Move to back
                                    </ListItemText>
                                </MenuItem>
                            </>
                        )}
                    </MenuList>
                </Menu>
            </div>
        );
    }
}
