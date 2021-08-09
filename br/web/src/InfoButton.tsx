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

import Badge from '@material-ui/core/Badge';
import IconButton from '@material-ui/core/IconButton';
import InfoIcon from '@material-ui/icons/InfoOutlined';
import * as React from 'react';
import { Link } from 'react-router-dom';

import * as api from './api';


interface Props {
    taskQueue: api.TaskQueue
}

export default class InfoButton extends React.Component<Props> {
    render() {
        return (
            <div>
                <IconButton color='inherit' title='Task info' component={Link} to='/tasks'>
                    <Badge badgeContent={this.props.taskQueue.queue.length} color='primary'>
                        <InfoIcon />
                    </Badge>
                </IconButton>
            </div>
        );
    }
}
