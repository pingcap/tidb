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

import { createStyles, lighten, Theme, WithStyles, withStyles } from '@material-ui/core/styles';
import * as React from 'react';

import * as api from './api';


const styles = (theme: Theme) => createStyles({
    progressDot: {
        width: 8,
        height: 8,
        display: 'inline-block',
        marginRight: 8,
        borderRadius: '50%',
    },
    filled: {},
    empty: {},
    primary: {
        '&$filled': {
            backgroundColor: theme.palette.primary.main,
        },
        '&$empty': {
            backgroundColor: lighten(theme.palette.primary.main, 0.6),
        },
    },
    error: {
        '&$filled': {
            backgroundColor: theme.palette.error.main,
        },
        '&$empty': {
            backgroundColor: lighten(theme.palette.error.main, 0.6),
        },
    },
});

interface Props extends WithStyles<typeof styles> {
    total: number
    status: api.CheckpointStatus
}

class DottedProgress extends React.Component<Props> {
    render() {
        const { classes } = this.props;

        const status = this.props.status;
        const colorClass = status <= api.CheckpointStatus.MaxInvalid ? classes.error : classes.primary;
        const step = api.stepOfCheckpointStatus(status);

        return (
            <div>
                {Array.from({ length: this.props.total }).map((_, i) => (
                    <div key={i} className={`${classes.progressDot} ${colorClass} ${i < step ? classes.filled : classes.empty}`} />
                ))}
                {api.labelOfCheckpointStatus(status)}
            </div>
        );
    }
}

export default withStyles(styles)(DottedProgress);
