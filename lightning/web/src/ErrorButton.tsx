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

import Dialog from '@material-ui/core/Dialog';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogTitle from '@material-ui/core/DialogTitle';
import IconButton from '@material-ui/core/IconButton';
import { createStyles, Theme, WithStyles, withStyles } from '@material-ui/core/styles';
import WarningIcon from '@material-ui/icons/Warning';
import * as React from 'react';


const styles = (theme: Theme) => createStyles({
    stackTrace: {
        whiteSpace: 'pre',
        fontSize: theme.typography.caption.fontSize,
    },
});

interface Props extends WithStyles<typeof styles> {
    lastError: string
    color?: 'inherit'
}

interface States {
    dialogOpened: boolean
}

class ErrorButton extends React.Component<Props, States> {
    constructor(props: Props) {
        super(props);

        this.state = {
            dialogOpened: false,
        };
    }

    handleOpenDialog = () => this.setState({ dialogOpened: true });

    handleCloseDialog = () => this.setState({ dialogOpened: false });

    render() {
        const { classes } = this.props;

        let firstLine: string
        let restLines: string
        const firstLineBreak = this.props.lastError.indexOf('\n');
        if (firstLineBreak >= 0) {
            firstLine = this.props.lastError.substr(0, firstLineBreak);
            restLines = this.props.lastError.substr(firstLineBreak + 1);
        } else {
            firstLine = this.props.lastError;
            restLines = '';
        }

        return (
            <>
                <IconButton onClick={this.handleOpenDialog} color={this.props.color} title='Show last error message'>
                    <WarningIcon />
                </IconButton>
                <Dialog open={this.state.dialogOpened} onClose={this.handleCloseDialog} maxWidth='lg'>
                    <DialogTitle>{firstLine}</DialogTitle>
                    <DialogContent>
                        <DialogContentText className={classes.stackTrace}>
                            {restLines}
                        </DialogContentText>
                    </DialogContent>
                </Dialog>
            </>
        );
    }
}

export default withStyles(styles)(ErrorButton);
