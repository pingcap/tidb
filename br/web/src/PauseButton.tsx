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

import IconButton from '@material-ui/core/IconButton';
import PauseIcon from '@material-ui/icons/Pause';
import PlayArrowIcon from '@material-ui/icons/PlayArrow';
import * as React from 'react';


interface Props {
    paused: boolean
    onTogglePaused: () => void
}

export default class PauseButton extends React.Component<Props> {
    render() {
        return (
            <IconButton color='inherit' title={this.props.paused ? 'Resume' : 'Pause'} onClick={this.props.onTogglePaused} disableTouchRipple>
                {this.props.paused ? <PlayArrowIcon /> : <PauseIcon />}
            </IconButton>
        );
    }
}
